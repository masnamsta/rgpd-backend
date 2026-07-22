// Backend RGPD — passerelle vers les API Judilibre (Cour de cassation)
// et Légifrance (DILA), toutes deux exposées via le portail PISTE.
//
// AJOUT : un cache mémoire alimenté par une récupération PAGINÉE de TOUTES
// les décisions Judilibre liées au RGPD, rafraîchi automatiquement via cron.
// CORRECTIF : gestion d'erreur isolée par requête + plafond MAX_PAGES pour
// éviter d'atteindre la limite de pagination de Judilibre (erreur 416).

require('dotenv').config();
const express = require('express');
const cors = require('cors');
const cron = require('node-cron');

const app = express();
app.use(express.static('public'));
app.use(cors());
app.use(express.json());
const ENV = process.env.PISTE_ENV || 'sandbox';

const CONFIG = {
  sandbox: {
    oauthUrl: 'https://sandbox-oauth.piste.gouv.fr/api/oauth/token',
    judilibreBase: 'https://sandbox-api.piste.gouv.fr/cassation/judilibre/v1.0',
    legifranceBase: 'https://sandbox-api.piste.gouv.fr/dila/legifrance/lf-engine-app',
  },
  production: {
    oauthUrl: 'https://oauth.piste.gouv.fr/api/oauth/token',
    judilibreBase: 'https://api.piste.gouv.fr/cassation/judilibre/v1.0',
    legifranceBase: 'https://api.piste.gouv.fr/dila/legifrance/lf-engine-app',
  },
};

const { oauthUrl, judilibreBase, legifranceBase } = CONFIG[ENV];

let cachedToken = null;
let tokenExpiresAt = 0;

async function getAccessToken() {
  const now = Date.now();
  if (cachedToken && now < tokenExpiresAt) {
    return cachedToken;
  }

  const body = new URLSearchParams({
    grant_type: 'client_credentials',
    client_id: process.env.PISTE_CLIENT_ID,
    client_secret: process.env.PISTE_CLIENT_SECRET,
    scope: 'openid',
  });

  const res = await fetch(oauthUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body,
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Échec de l'authentification PISTE (${res.status}): ${text}`);
  }

  const data = await res.json();
  cachedToken = data.access_token;
  tokenExpiresAt = now + (data.expires_in - 60) * 1000;
  return cachedToken;
}

async function chercherJudilibre({ query, jurisdiction, page = 0, page_size = 10 }) {
  const token = await getAccessToken();
  const params = new URLSearchParams({
    query,
    page,
    page_size,
    ...(jurisdiction ? { jurisdiction } : {}),
  });

  const apiRes = await fetch(`${judilibreBase}/search?${params}`, {
    headers: {
      Authorization: `Bearer ${token}`,
      'KeyId': process.env.PISTE_JUDILIBRE_KEY_ID,
    },
  });

  if (!apiRes.ok) {
    const text = await apiRes.text();
    throw new Error(`Erreur Judilibre (${apiRes.status}): ${text}`);
  }

  return apiRes.json();
}
const THEMES_RGPD = {
  violations: ['violation', 'fuite de données', 'article 33', 'article 34', 'notification cnil'],
  sous_traitance: ['sous-traitant', 'sous-traitance', 'article 28', 'responsable de traitement'],
  transferts: ['transfert', 'pays tiers', 'clauses contractuelles types', 'hors union européenne'],
  securite: ['mesure technique', 'chiffrement', 'authentification', 'accès non autorisé', 'sécurité des données'],
  droits_personnes: ["droit d'accès", "droit à l'oubli", "droit d'opposition", 'rectification', 'effacement'],
  ia: ['intelligence artificielle', 'algorithme', 'traitement automatisé', 'décision automatisée'],
};

function classifierDecision(d) {
  const texte = `${d.summary || ''} ${(d.themes || []).join(' ')}`.toLowerCase();
  const detectes = Object.entries(THEMES_RGPD)
    .filter(([, mots]) => mots.some((m) => texte.includes(m)))
    .map(([cle]) => cle);
  return detectes.length ? detectes : ['autre'];
}
function formaterDecision(d) {
  return {
    id: d.id,
    juridiction: d.jurisdiction,
    chambre: d.chamber || null,
    numero: d.number,
    date: d.decision_date,
    titre: d.summary || d.solution || null,
    themes: d.themes || [],
    themesRgpd: classifierDecision(d),
    url: `https://www.courdecassation.fr/decision/${d.id}`,
  };
}

const REQUETES_RGPD = [
  'RGPD',
  'données personnelles',
  'protection des données',
  "règlement général sur la protection des données",
];
const TAILLE_PAGE = 10;
const PAUSE_MS = 250;
const MAX_PAGES = 20;

let cacheDecisions = [];
let derniereMiseAJour = null;
let rafraichissementEnCours = false;

function pause(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function rafraichirCacheRGPD() {
  if (rafraichissementEnCours) return;
  rafraichissementEnCours = true;

  const vus = new Map();

  for (const query of REQUETES_RGPD) {
    let page = 0;
    let total = Infinity;

    while (page * TAILLE_PAGE < total && page < MAX_PAGES) {
      try {
        const data = await chercherJudilibre({ query, page, page_size: TAILLE_PAGE });
        total = data.total || 0;
        const resultats = data.results || [];

        resultats.forEach((d) => {
          if (!vus.has(d.id)) {
            vus.set(d.id, formaterDecision(d));
          }
        });

        if (resultats.length === 0) break;
        page++;
        await pause(PAUSE_MS);
      } catch (err) {
        console.error(`Erreur Judilibre pour la requête "${query}" (page ${page}) :`, err.message);
        break;
      }
    }
  }

await rafraichirCacheLegifrance();
cacheDecisions = [...Array.from(vus.values()).map(d => ({ ...d, source: 'judilibre' })), ...cacheLegifrance];
  derniereMiseAJour = new Date().toISOString();
  console.log(`[${derniereMiseAJour}] Cache RGPD rafraîchi : ${cacheDecisions.length} décisions uniques`);
  rafraichissementEnCours = false;
}
async function chercherLegifrance(fond, motCle, page = 1) {
  const token = await getAccessToken();
  const body = {
    fond,
    recherche: {
      champs: [{ criteres: [{ valeur: motCle, proximite: 2, operateur: 'ET', typeRecherche: 'UN_DES_MOTS' }], operateur: 'ET', typeChamp: 'ALL' }],
      pageSize: 20,
      pageNumber: page,
      operateur: 'ET',
      typePagination: 'DEFAUT',
      sort: 'PERTINENCE',
    },
  };
  const apiRes = await fetch(`${legifranceBase}/search`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!apiRes.ok) {
    const text = await apiRes.text();
    throw new Error(`Erreur Légifrance (${apiRes.status}): ${text}`);
  }
  return apiRes.json();
}

function extraireDateTitre(titre) {
  const m = (titre || '').match(/(\d{2})\/(\d{2})\/(\d{4})/);
  return m ? `${m[3]}-${m[2]}-${m[1]}` : null;
}

function formaterDecisionLegifrance(d, fond) {
  const t = (d.titles && d.titles[0]) || {};
  const titre = t.title || '';
  let resume = (d.resumePrincipal || []).join(' ');
  if (!resume && d.text) {
    resume = d.text.replace(/<[^>]+>/g, '').replace(/\[\.\.\.\]/g, ' ').trim();
  }
  return {
    id: t.id || d.id,
    juridiction: fond === 'CETAT' ? 'ce' : 'cnil',
    chambre: null,
    numero: t.id || null,
    date: d.datePublication ? d.datePublication.slice(0, 10) : extraireDateTitre(titre),
    titre: titre,
    themes: resume ? [resume.slice(0, 220).trim() + (resume.length > 220 ? '…' : '')] : [],
    themesRgpd: classifierDecision({ summary: `${titre} ${resume}` }),
    source: 'legifrance',
    url: `https://www.legifrance.gouv.fr/${fond === 'CETAT' ? 'ceta' : 'cnil'}/id/${t.id || d.id}`,
  };
}
let cacheLegifrance = [];

async function rafraichirCacheLegifrance() {
  const requetesRGPD = ['données personnelles', 'RGPD'];
  const fonds = ['CETAT', 'CNIL'];
  const vus = new Map();

  for (const fond of fonds) {
    for (const motCle of requetesRGPD) {
      try {
        const data = await chercherLegifrance(fond, motCle);
        const resultats = data.results || [];
        resultats.forEach((d) => {
          const f = formaterDecisionLegifrance(d, fond);
          if (f.id && !vus.has(f.id)) vus.set(f.id, f);
        });
        await pause(PAUSE_MS);
      } catch (err) {
        console.error(`Erreur Légifrance (${fond}, "${motCle}") :`, err.message);
      }
    }
  }
  cacheLegifrance = Array.from(vus.values());
  console.log(`Cache Légifrance rafraîchi : ${cacheLegifrance.length} décisions`);
}
cron.schedule('0 */6 * * *', rafraichirCacheRGPD);
rafraichirCacheRGPD();

app.get('/api/jurisprudence', async (req, res) => {
  try {
    const { query = '', jurisdiction, page = 0, page_size = 10, live } = req.query;

    if (!live) {
      return res.json({
        total: cacheDecisions.length,
        derniere_maj: derniereMiseAJour,
        results: cacheDecisions,
      });
    }

    const data = await chercherJudilibre({ query, jurisdiction, page, page_size });
    const results = (data.results || []).map(formaterDecision);
    res.json({ total: data.total, results });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Erreur serveur', detail: err.message });
  }
});

app.post('/api/jurisprudence/refresh', async (req, res) => {
  await rafraichirCacheRGPD();
  res.json({ ok: true, total: cacheDecisions.length, derniere_maj: derniereMiseAJour });
});

app.get('/api/texte/:legiartiId', async (req, res) => {
  try {
    const token = await getAccessToken();

    const apiRes = await fetch(`${legifranceBase}/consult/getArticle`, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${token}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ id: req.params.legiartiId }),
    });

    if (!apiRes.ok) {
      const text = await apiRes.text();
      return res.status(apiRes.status).json({ error: 'Erreur Légifrance', detail: text });
    }

    const data = await apiRes.json();
    res.json({
      id: data.article?.id,
      texte: data.article?.texte,
      etat: data.article?.etat,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Erreur serveur', detail: err.message });
  }
});

app.post('/api/textes/recherche', async (req, res) => {
  try {
    const { motCle, fond = 'CODE_DATE' } = req.body;
    const token = await getAccessToken();

    const apiRes = await fetch(`${legifranceBase}/search`, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${token}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        recherche: {
          champs: [{ typeChamp: 'ALL', criteres: [{ typeRecherche: 'UN_DES_MOTS', valeur: motCle }] }],
          pageNumber: 1,
          pageSize: 10,
          sort: 'PERTINENCE',
        },
        fond,
      }),
    });

    if (!apiRes.ok) {
      const text = await apiRes.text();
      return res.status(apiRes.status).json({ error: 'Erreur Légifrance', detail: text });
    }

    res.json(await apiRes.json());
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Erreur serveur', detail: err.message });
  }
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
  console.log(`Backend RGPD démarré sur le port ${PORT} (environnement PISTE: ${ENV})`);
});
