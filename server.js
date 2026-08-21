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
const { Pool } = require('pg');

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
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

async function initDbRGPD() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS decisions_rgpd (
      id TEXT PRIMARY KEY,
      juridiction TEXT,
      chambre TEXT,
      numero TEXT,
      date TEXT,
      titre TEXT,
      themes JSONB,
      themes_rgpd JSONB,
      source TEXT,
      url TEXT,
      updated_at TIMESTAMP DEFAULT NOW()
    );
  `);
  console.log('Table decisions_rgpd prête.');
}

async function sauvegarderDecisions(decisions) {
  for (const d of decisions) {
    try {
      await pool.query(
        `INSERT INTO decisions_rgpd (id, juridiction, chambre, numero, date, titre, themes, themes_rgpd, source, url, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())
         ON CONFLICT (id) DO UPDATE SET
           juridiction = EXCLUDED.juridiction,
           chambre = EXCLUDED.chambre,
           numero = EXCLUDED.numero,
           date = EXCLUDED.date,
           titre = EXCLUDED.titre,
           themes = EXCLUDED.themes,
           themes_rgpd = EXCLUDED.themes_rgpd,
           source = EXCLUDED.source,
           url = EXCLUDED.url,
           updated_at = NOW();`,
        [
          d.id,
          d.juridiction || null,
          d.chambre || null,
          d.numero || null,
          d.date || null,
          d.titre || null,
          JSON.stringify(d.themes || []),
          JSON.stringify(d.themesRgpd || []),
          d.source || null,
          d.url || null,
        ]
      );
    } catch (err) {
      console.error(`Erreur sauvegarde décision ${d.id} :`, err.message);
    }
  }
  console.log(`${decisions.length} décisions sauvegardées en base.`);
}
async function chargerCacheDepuisDB() {
  const { rows } = await pool.query('SELECT * FROM decisions_rgpd');
  cacheDecisions = rows.map((r) => ({
    id: r.id,
    juridiction: r.juridiction,
    chambre: r.chambre,
    numero: r.numero,
    date: r.date,
    titre: r.titre,
    themes: r.themes || [],
    themesRgpd: r.themes_rgpd || [],
    source: r.source,
    url: r.url,
  }));

  cacheDecisions.sort((a, b) => new Date(b.date || 0) - new Date(a.date || 0));

  const { rows: maxRows } = await pool.query('SELECT MAX(updated_at) AS max FROM decisions_rgpd');
  derniereMiseAJour = maxRows[0].max ? new Date(maxRows[0].max).toISOString() : null;

  console.log(`Cache chargé depuis la base : ${cacheDecisions.length} décisions (dernière maj : ${derniereMiseAJour})`);
}
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

async function chercherJudilibre({ query, jurisdiction, page = 0, page_size = 10, sort, order, date_start, date_end }) {
  const token = await getAccessToken();
  const params = new URLSearchParams();
  params.append('query', query);
  params.append('page', page);
  params.append('page_size', page_size);
  if (sort) params.append('sort', sort);
  if (order) params.append('order', order);
  if (date_start) params.append('date_start', date_start);
  if (date_end) params.append('date_end', date_end);

  const jurisdictions = jurisdiction
    ? (Array.isArray(jurisdiction) ? jurisdiction : [jurisdiction])
    : ['cc', 'ca'];

  jurisdictions.forEach((j) => params.append('jurisdiction', j));

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

// CORRECTIF : filtre de pertinence appliqué aux résultats Judilibre.
// La recherche plein texte de l'API Judilibre matche des mots isolés
// (ex : "traitement" au sens salarial dans un litige de droit du travail),
// ce qui pollue le cache avec des décisions sans rapport avec le RGPD.
// On ne retient donc que les décisions dont le résumé/les thèmes officiels
// (les seuls champs enrichis disponibles) contiennent un terme RGPD
// reconnaissable. Les décisions non enrichies (summary/themes vides), qui
// ne peuvent de toute façon pas être classifiées de façon fiable, sont
// écartées plutôt que de tomber dans la catégorie "autre".
// CORRECTIF (issue #1) : ajout de d.solution comme source de texte
// supplémentaire. Beaucoup de décisions Judilibre n'ont pas de "summary"
// (sommaire officiel) rempli mais ont un champ "solution" renseigné ; ces
// décisions étaient rejetées à tort avant ce correctif.
const MOTS_CLES_PERTINENCE_RGPD = [
  'donnée personnelle', 'données personnelles',
  'donnée à caractère personnel', 'données à caractère personnel',
  'rgpd', 'cnil', 'protection des données',
  'informatique et libertés', 'traitement de données',
  'responsable du traitement', 'responsable de traitement',
  'sous-traitant', 'sous-traitance',
  "droit d'accès aux données", "droit à l'oubli", "droit d'opposition",
  'violation de données', 'transfert de données', 'transfert hors union européenne',
  'délégué à la protection des données', 'vidéosurveillance', 'géolocalisation',
  'biométrie', 'décision automatisée', 'consentement au traitement',
  // AJOUT (issue #2) : vocabulaire RGPD complémentaire pour élargir le filtre
  // de pertinence, qui reposait jusqu'ici sur seulement 24 expressions et
  // excluait des décisions pourtant clairement liées au RGPD (ex : arrêts
  // parlant de "base légale" ou de "durée de conservation" sans jamais
  // employer littéralement "RGPD" ou "données personnelles").
  'base légale du traitement', 'intérêt légitime', 'minimisation des données',
  'durée de conservation des données', 'profilage', 'cookies',
  'finalité du traitement', 'registre des traitements',
  "analyse d'impact relative à la protection des données", 'aipd',
  'protection des données dès la conception', 'privacy by design',
  'anonymisation', 'pseudonymisation', 'exercice des droits',
  'plainte auprès de la cnil', 'mise en demeure de la cnil',
  'amende administrative', 'sanction pécuniaire de la cnil',
  'autorité de contrôle', 'personne concernée', 'flux transfrontière de données',
  'clauses contractuelles types', 'données de santé', 'données sensibles',
];

function estPertinentRGPD(d) {
  const texte = `${d.summary || ''} ${d.solution || ''} ${(d.themes || []).join(' ')}`.toLowerCase();
  if (!texte.trim()) return false;
  return MOTS_CLES_PERTINENCE_RGPD.some((mot) => texte.includes(mot));
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
  'données à caractère personnel',
  'consentement au traitement',
  "droit à l'effacement",
  "droit à l'oubli",
  "droit d'accès aux données",
  'violation de données',
  'vidéosurveillance',
  'cookies',
  'sous-traitant',
  'transfert de données hors UE',
  'délégué à la protection des données',
  'CNIL',
  'traitement automatisé',
  'biométrie',
  'géolocalisation',
  'responsable de traitement',
  'sanction CNIL',
  "droit d'opposition",
  'décision automatisée',
  'intelligence artificielle',
  'loi informatique et libertés',
  // AJOUT (issue #2) : requêtes supplémentaires pour élargir le net de
  // recherche Judilibre à des notions RGPD qui n'étaient interrogées par
  // aucun des termes précédents.
  'base légale du traitement',
  'intérêt légitime',
  'minimisation des données',
  'analyse d\'impact relative à la protection des données',
  'anonymisation données personnelles',
  'pseudonymisation',
  'données de santé',
  'profilage',
];
const TAILLE_PAGE = 10;
const PAUSE_MS = 350;
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

  try {
    const vus = new Map();
    let rejetes = 0;

    let dateDepuis = null;
    if (derniereMiseAJour) {
      const marge = new Date(derniereMiseAJour);
      marge.setDate(marge.getDate() - 8);
      dateDepuis = marge.toISOString().slice(0, 10);
    }

    for (const query of REQUETES_RGPD) {
      let page = 0;
      let total = Infinity;

      while (page * TAILLE_PAGE < total && page < MAX_PAGES) {
        try {
          const data = await chercherJudilibre({
            query,
            page,
            page_size: TAILLE_PAGE,
            sort: 'date',
            order: 'desc',
            date_start: dateDepuis,
          });
          total = data.total || 0;
          const resultats = data.results || [];

          resultats.forEach((d) => {
            if (!estPertinentRGPD(d)) {
              rejetes++;
              return;
            }
            if (!vus.has(d.id)) {
              vus.set(d.id, formaterDecision(d));
            }
          });
        console.log(`Judilibre "${query}" page ${page} : ${resultats.length} résultats (total: ${total})`);
          if (resultats.length === 0) break;
          page++;
          await pause(PAUSE_MS);
        } catch (err) {
          console.error(`Erreur Judilibre pour la requête "${query}" (page ${page}) :`, err.message);
          break;
        }
      }
    }

    console.log(`Filtre de pertinence Judilibre : ${rejetes} décisions écartées (summary/solution/themes vides ou hors sujet RGPD)`);

    try {
      await rafraichirCacheLegifrance();
    } catch (err) {
      console.error('Erreur globale rafraîchissement Légifrance :', err.message);
    }

    try {
      await rafraichirCacheCJUE();
    } catch (err) {
      console.error('Erreur globale rafraîchissement CJUE :', err.message);
    }

    const cacheParId = new Map(cacheDecisions.map((d) => [d.id, d]));
    Array.from(vus.values()).forEach((d) => cacheParId.set(d.id, { ...d, source: 'judilibre' }));
    cacheLegifrance.forEach((d) => cacheParId.set(d.id, d));
    cacheCJUE.forEach((d) => cacheParId.set(d.id, d));

    // CORRECTIF : purge retroactive. Le filtre estPertinentRGPD() ne
    // s'appliquait jusqu'ici qu'aux nouvelles decisions Judilibre recuperees
    // a ce cycle (vus) -- les decisions Judilibre non pertinentes deja
    // presentes en cache/en base (accumulees avant ce filtre) restaient donc
    // indefiniment. On repasse ici TOUTES les decisions Judilibre du cache
    // fusionne par le meme filtre, et on supprime de la base celles qui ne
    // passent pas.
    let fusionne = Array.from(cacheParId.values());
    const idsAPurger = [];
    fusionne = fusionne.filter((d) => {
      if (d.source !== 'judilibre') return true;
      const pertinent = estPertinentRGPD({ summary: d.titre, themes: d.themes });
      if (!pertinent) idsAPurger.push(d.id);
      return pertinent;
    });
    if (idsAPurger.length) {
      try {
        await pool.query('DELETE FROM decisions_rgpd WHERE id = ANY($1)', [idsAPurger]);
        console.log(`Purge retroactive : ${idsAPurger.length} anciennes decisions Judilibre non pertinentes supprimees.`);
      } catch (err) {
        console.error('Erreur purge decisions non pertinentes :', err.message);
      }
    }

    cacheDecisions = fusionne;
    cacheDecisions.sort((a, b) => new Date(b.date || 0) - new Date(a.date || 0));
    derniereMiseAJour = new Date().toISOString();
    console.log(`[${derniereMiseAJour}] Cache RGPD rafraîchi : ${cacheDecisions.length} décisions uniques (dont ${vus.size} nouvelles/mises à jour Judilibre)`);
        await sauvegarderDecisions(cacheDecisions);
  } finally {
    rafraichissementEnCours = false;
  }
}
async function chercherLegifrance(fond, motCle, page = 1, pageSize = 20) {
  const token = await getAccessToken();
  const body = {
    fond,
    recherche: {
      champs: [{ criteres: [{ valeur: motCle, proximite: 2, operateur: 'ET', typeRecherche: 'UN_DES_MOTS' }], operateur: 'ET', typeChamp: 'ALL' }],
      pageSize: pageSize,
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

const TAILLE_PAGE_LEGIFRANCE = 20;

async function rafraichirCacheLegifrance() {
 const requetesRGPD = [
  'RGPD',
  'données personnelles',
  'protection des données',
  "règlement général sur la protection des données",
  'données à caractère personnel',
  'consentement au traitement',
  "droit à l'effacement",
  "droit à l'oubli",
  "droit d'accès aux données",
  'violation de données',
  'vidéosurveillance',
  'cookies',
  'sous-traitant',
  'transfert de données hors Union européenne',
  'délégué à la protection des données',
  'CNIL',
  'traitement automatisé',
  'biométrie',
  'géolocalisation',
  'responsable de traitement',
  'sanction CNIL',
  "droit d'opposition",
  'décision automatisée',
  'intelligence artificielle',
  'loi informatique et libertés',
];
  const fonds = ['CETAT', 'CNIL'];
  const vus = new Map();

  for (const fond of fonds) {
    for (const motCle of requetesRGPD) {
      let page = 1;
      let total = Infinity;

      while ((page - 1) * TAILLE_PAGE_LEGIFRANCE < total && page <= MAX_PAGES) {
        try {
          const data = await chercherLegifrance(fond, motCle, page, TAILLE_PAGE_LEGIFRANCE);
          total = data.totalResultNumber || 0;
          const resultats = data.results || [];

          resultats.forEach((d) => {
            const f = formaterDecisionLegifrance(d, fond);
            if (f.id && !vus.has(f.id)) vus.set(f.id, f);
          });
          console.log(`Légifrance ${fond} "${motCle}" page ${page} : ${resultats.length} résultats`);
          if (resultats.length === 0) break;
          page++;
          await pause(PAUSE_MS);
        } catch (err) {
          console.error(`Erreur Légifrance (${fond}, "${motCle}", page ${page}) :`, err.message);
          break;
        }
      }
    }
  }
  cacheLegifrance = Array.from(vus.values());
  console.log(`Cache Légifrance rafraîchi : ${cacheLegifrance.length} décisions`);
}
function extraireNumeroAffaire(titre) {
  const segments = titre.split('#');
  const dernier = segments[segments.length - 1] || '';
  return dernier.replace(/^Affaire[s]?\s*(jointes)?\s*/i, '').replace('.', '').trim();
}

function formaterDecisionCJUE(b) {
  const id = b.work.value;
  const titreComplet = b.title.value;
  const segments = titreComplet.split('#');
  return {
    id,
    juridiction: 'cjue',
    chambre: null,
    numero: extraireNumeroAffaire(titreComplet),
    date: b.date.value,
    titre: segments[0] || titreComplet,
    themes: segments[2] ? [segments[2].slice(0, 220).trim() + (segments[2].length > 220 ? '…' : '')] : [],
    themesRgpd: classifierDecision({ summary: titreComplet }),
    source: 'cjue',
    url: id,
  };
}

async function chercherCJUE(motCle) {
  const sparqlQuery = `
    PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
    SELECT DISTINCT ?work ?title ?date WHERE {
      ?work cdm:work_has_resource-type <http://publications.europa.eu/resource/authority/resource-type/CASE_LAW> .
      ?work cdm:work_date_document ?date .
      ?exp cdm:expression_belongs_to_work ?work .
      ?exp cdm:expression_title ?title .
      ?exp cdm:expression_uses_language <http://publications.europa.eu/resource/authority/language/FRA> .
      FILTER(CONTAINS(LCASE(?title), LCASE("${motCle}")))
    } ORDER BY DESC(?date) LIMIT 100
  `;

  const apiRes = await fetch('https://publications.europa.eu/webapi/rdf/sparql', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Accept': 'application/sparql-results+json' },
    body: new URLSearchParams({ query: sparqlQuery }),
  });

  if (!apiRes.ok) {
    const text = await apiRes.text();
    throw new Error(`Erreur CJUE (${apiRes.status}): ${text}`);
  }

  const data = await apiRes.json();
  return data.results.bindings;
}

let cacheCJUE = [];

async function rafraichirCacheCJUE() {
  const requetesCJUE = ['protection des données', 'données à caractère personnel', 'RGPD'];
  const vus = new Map();

  for (const motCle of requetesCJUE) {
    try {
      const bindings = await chercherCJUE(motCle);
      bindings.forEach((b) => {
        const f = formaterDecisionCJUE(b);
        if (!vus.has(f.id)) vus.set(f.id, f);
      });
      await pause(PAUSE_MS);
    } catch (err) {
      console.error(`Erreur CJUE ("${motCle}") :`, err.message);
    }
  }
  cacheCJUE = Array.from(vus.values());
  console.log(`Cache CJUE rafraîchi : ${cacheCJUE.length} décisions`);
}
initDbRGPD().then(async () => {
  await chargerCacheDepuisDB();
  cron.schedule('0 */6 * * *', rafraichirCacheRGPD);
  rafraichirCacheRGPD();
});

// AJOUT : mappe la juridiction brute d'une décision (cc, ca, cjue, ce,
// cnil...) vers le même code court utilisé côté frontend pour les boutons
// de filtre (cass, ca, cjue, ce, cnil, autre), afin que le filtre
// juridiction s'applique sur tout le cache et pas seulement sur la page déjà
// chargée dans le navigateur.
function courtDepuisJuridiction(juridiction) {
  const j = (juridiction || '').toLowerCase();
  if (j === 'cjue' || j.includes('cour de justice')) return 'cjue';
  if (j === 'cc' || j.includes('cass')) return 'cass';
  if (j === 'ca' || j.includes('appel')) return 'ca';
  if (j === 'ce') return 'ce';
  if (j === 'cnil') return 'cnil';
  return 'autre';
}

app.get('/api/jurisprudence', async (req, res) => {
  try {
    const { query = '', jurisdiction, page = 0, page_size = 24, live, court, theme } = req.query;

    if (!live) {
      let resultatsFiltres = cacheDecisions;
      const motCle = (query || '').toLowerCase().trim();
      if (motCle && motCle !== 'rgpd') {
        resultatsFiltres = cacheDecisions.filter(d =>
          (d.titre && d.titre.toLowerCase().includes(motCle)) ||
          (d.sommaire && d.sommaire.toLowerCase().includes(motCle)) ||
          (d.numero && d.numero.toLowerCase().includes(motCle)) ||
          (d.themesRgpd && d.themesRgpd.join(' ').toLowerCase().includes(motCle))
        );
      }

      // AJOUT : filtres juridiction (court) et thème RGPD (theme), appliqués
      // sur l'ensemble des résultats déjà filtrés par mot-clé, avant la
      // pagination. Auparavant ces filtres n'existaient que côté client et
      // ne portaient que sur les décisions déjà chargées dans le navigateur.
      if (court && court !== 'all') {
        resultatsFiltres = resultatsFiltres.filter(d => courtDepuisJuridiction(d.juridiction) === court);
      }
      if (theme && theme !== 'all') {
        resultatsFiltres = resultatsFiltres.filter(d => (d.themesRgpd || []).includes(theme));
      }

      const pageNum = Math.max(0, parseInt(page, 10) || 0);
      const pageSizeNum = Math.max(1, parseInt(page_size, 10) || 24);
      const debut = pageNum * pageSizeNum;
      const resultatsPage = resultatsFiltres.slice(debut, debut + pageSizeNum);

      return res.json({
        total: resultatsFiltres.length,
        derniere_maj: derniereMiseAJour,
        results: resultatsPage,
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
