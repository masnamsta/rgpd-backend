// Backend RGPD — passerelle vers les API Judilibre (Cour de cassation)
// et Légifrance (DILA), toutes deux exposées via le portail PISTE.
//
// AJOUT : un cache mémoire alimenté par une récupération PAGINÉE de TOUTES
// les décisions Judilibre liées au RGPD, rafraîchi automatiquement via cron.
// Les routes Légifrance et la logique OAuth2 d'origine sont inchangées.

require('dotenv').config();
const express = require('express');
const cors = require('cors');
const cron = require('node-cron');

const app = express();
app.use(cors());
app.use(express.json());

const ENV = process.env.PISTE_ENV || 'sandbox'; // 'sandbox' ou 'production'

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

// ---------------------------------------------------------------------
// Gestion du token OAuth2 (inchangé)
// ---------------------------------------------------------------------
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

// ---------------------------------------------------------------------
// Fonction utilitaire : un seul appel à Judilibre /search
// ---------------------------------------------------------------------
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

function formaterDecision(d) {
  return {
    id: d.id,
    juridiction: d.jurisdiction,
    chambre: d.chamber || null,
    numero: d.number,
    date: d.decision_date,
    titre: d.summary || d.solution || null,
    themes: d.themes || [],
    url: `https://www.courdecassation.fr/decision/${d.id}`,
  };
}

// ---------------------------------------------------------------------
// NOUVEAU : récupération PAGINÉE de toutes les décisions RGPD, sur
// plusieurs formulations de requête, avec déduplication par id.
// ---------------------------------------------------------------------
const REQUETES_RGPD = [
  'RGPD',
  'données personnelles',
  'protection des données',
  "règlement général sur la protection des données",
];
const TAILLE_PAGE = 10;
const PAUSE_MS = 250;

let cacheDecisions = [];
let derniereMiseAJour = null;
let rafraichissementEnCours = false;

function pause(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function rafraichirCacheRGPD() {
  if (rafraichissementEnCours) return;
  rafraichissementEnCours = true;

  try {
    const vus = new Map();

    for (const query of REQUETES_RGPD) {
      let page = 0;
      let total = Infinity;

      while (page * TAILLE_PAGE < total) {
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
      }
    }

    cacheDecisions = Array.from(vus.values());
    derniereMiseAJour = new Date().toISOString();
    console.log(`[${derniereMiseAJour}] Cache RGPD rafraîchi : ${cacheDecisions.length} décisions uniques`);
  } catch (err) {
    console.error('Erreur lors du rafraîchissement du cache RGPD :', err.message);
  } finally {
    rafraichissementEnCours = false;
  }
}

cron.schedule('0 */6 * * *', rafraichirCacheRGPD);
rafraichirCacheRGPD();

// ---------------------------------------------------------------------
// Route 1 — Recherche de jurisprudence via Judilibre
// ---------------------------------------------------------------------
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

// ---------------------------------------------------------------------
// NOUVEAU — Forcer manuellement un rafraîchissement complet du cache
// ---------------------------------------------------------------------
app.post('/api/jurisprudence/refresh', async (req, res) => {
  await rafraichirCacheRGPD();
  res.json({ ok: true, total: cacheDecisions.length, derniere_maj: derniereMiseAJour });
});

// ---------------------------------------------------------------------
// Route 2 — Récupération d'un article de loi via Légifrance (inchangé)
// ---------------------------------------------------------------------
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

// ---------------------------------------------------------------------
// Route 3 — Recherche plein texte Légifrance (inchangé)
// ---------------------------------------------------------------------
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
