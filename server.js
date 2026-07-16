// Backend RGPD — passerelle vers les API Judilibre (Cour de cassation)
// et Légifrance (DILA), toutes deux exposées via le portail PISTE.
//
// Le principe est le même pour les deux API :
//   1. On échange client_id + client_secret contre un access_token (OAuth2,
//      grant_type=client_credentials) auprès du serveur PISTE.
//   2. On réutilise ce token (il dure 1h) sur toutes les requêtes suivantes,
//      via l'en-tête Authorization: Bearer <token>.
//   3. On interroge les endpoints métier (/search, /consult/getArticle, etc.)
//
// Ce fichier expose deux routes propres pour ton frontend :
//   GET  /api/jurisprudence?query=...        -> proxy vers Judilibre /search
//   GET  /api/texte/:legiartiId               -> proxy vers Légifrance /consult/getArticle
//
// Le frontend n'a donc JAMAIS connaissance du client_secret : il ne parle
// qu'à ton propre backend, qui seul détient les identifiants PISTE.

require('dotenv').config();
const express = require('express');
const cors = require('cors');

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
// Gestion du token OAuth2 : on le met en cache mémoire et on ne le
// renouvelle qu'une fois expiré, pour ne pas spammer le serveur d'auth.
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
  // On retranche 60s de marge de sécurité avant l'expiration réelle
  tokenExpiresAt = now + (data.expires_in - 60) * 1000;
  return cachedToken;
}

// ---------------------------------------------------------------------
// Route 1 — Recherche de jurisprudence via Judilibre
// Exemple d'appel frontend : /api/jurisprudence?query=droit%20d%27acc%C3%A8s%20RGPD
// ---------------------------------------------------------------------
app.get('/api/jurisprudence', async (req, res) => {
  try {
    const { query = '', jurisdiction, page = 0, page_size = 10 } = req.query;
    const token = await getAccessToken();

    const params = new URLSearchParams({
      query,
      page,
      page_size,
      // filtre optionnel: 'cc' (Cour de cassation) ou 'ca' (cours d'appel)
      ...(jurisdiction ? { jurisdiction } : {}),
    });

    const apiRes = await fetch(`${judilibreBase}/search?${params}`, {
      headers: {
        Authorization: `Bearer ${token}`,
        'KeyId': process.env.PISTE_JUDILIBRE_KEY_ID, // fourni par PISTE
      },
    });

    if (!apiRes.ok) {
      const text = await apiRes.text();
      return res.status(apiRes.status).json({ error: 'Erreur Judilibre', detail: text });
    }

    const data = await apiRes.json();

    // On ne renvoie au frontend que les champs utiles, dans un format
    // proche de tes fiches d'arrêt (cite, titre, résumé, date, juridiction)
    const results = (data.results || []).map((d) => ({
      id: d.id,
      juridiction: d.jurisdiction,
      chambre: d.chamber || null,
      numero: d.number,
      date: d.decision_date,
      titre: d.summary || d.solution || null,
      themes: d.themes || [],
      url: `https://www.courdecassation.fr/decision/${d.id}`,
    }));

    res.json({ total: data.total, results });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Erreur serveur', detail: err.message });
  }
});

// ---------------------------------------------------------------------
// Route 2 — Récupération d'un article de loi via Légifrance
// Exemple d'appel frontend : /api/texte/LEGIARTI000037313109  (art. 15 loi I&L)
// L'identifiant LEGIARTI s'obtient au préalable via une recherche
// (POST /search sur l'API Légifrance) ; voir la route 3 ci-dessous.
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
// Route 3 — Recherche plein texte dans les fonds Légifrance (ex: CODE)
// Utile pour retrouver l'identifiant LEGIARTI d'un article avant de
// l'afficher avec la route ci-dessus.
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
