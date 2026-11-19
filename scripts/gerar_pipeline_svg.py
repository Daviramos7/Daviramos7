"""Gera assets/pipeline.svg: um 'ETL' animado dos dados públicos do GitHub do Davi.

Extract: API REST/GraphQL do GitHub · Transform: agregações em Python · Load: SVG no README.
Sem dependências externas (só stdlib).
"""
import json, os, sys, urllib.request
from collections import Counter
from datetime import datetime, timedelta, timezone

USER = os.environ.get("GH_USER", "Daviramos7")
TOKEN = os.environ.get("GITHUB_TOKEN", "")
OUT = sys.argv[1] if len(sys.argv) > 1 else "dist/pipeline.svg"
H = {"Accept": "application/vnd.github+json", "User-Agent": "pipeline-svg"}
if TOKEN:
    H["Authorization"] = f"Bearer {TOKEN}"


def get(url, data=None):
    req = urllib.request.Request(url, data=data, headers=H)
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


# ---------- EXTRACT ----------
user = get(f"https://api.github.com/users/{USER}")
repos, page = [], 1
while True:
    lote = get(f"https://api.github.com/users/{USER}/repos?per_page=100&type=owner&page={page}")
    repos += lote
    if len(lote) < 100:
        break
    page += 1
repos = [r for r in repos if not r["fork"]]

contribs = None
if TOKEN:
    try:
        q = {"query": "query($l:String!){user(login:$l){contributionsCollection{totalCommitContributions contributionCalendar{totalContributions}}}}",
             "variables": {"l": USER}}
        c = get("https://api.github.com/graphql", json.dumps(q).encode())["data"]["user"]["contributionsCollection"]
        contribs = c["contributionCalendar"]["totalContributions"]
    except Exception as e:  # GraphQL indisponível: cai para a busca de commits
        print("graphql falhou:", e, file=sys.stderr)
if contribs is None:
    desde = (datetime.now(timezone.utc) - timedelta(days=365)).date().isoformat()
    contribs = get(f"https://api.github.com/search/commits?q=author:{USER}+committer-date:>{desde}")["total_count"]

# ---------- TRANSFORM ----------
stars = sum(r["stargazers_count"] for r in repos)
langs = Counter(r["language"] for r in repos if r["language"])
top = langs.most_common(5)
total_l = sum(n for _, n in top) or 1
atualizado = max((r["pushed_at"] for r in repos), default="")[:10]
kpis = [("Repositórios", user["public_repos"]), ("Contribuições/ano", contribs),
        ("Estrelas", stars), ("Seguidores", user["followers"])]
hoje = datetime.now(timezone(timedelta(hours=-3))).strftime("%d/%m/%Y %H:%M")

# ---------- LOAD ----------
BG, CARD, TEAL, TXT, MUTED, LINE = "#0D1621", "#132232", "#19B5A5", "#FFFFFF", "#8B9BB4", "#34495E"
CORES = {"Python": "#3776AB", "TypeScript": "#3178C6", "JavaScript": "#F1E05A", "HTML": "#E34C26",
         "CSS": "#663399", "Kotlin": "#A97BFF", "Jupyter Notebook": "#DA5B0B", "Java": "#B07219", "C#": "#178600"}
W = 850
etapas = [("EXTRACT", "GitHub API"), ("TRANSFORM", "Python"), ("LOAD", "SVG"), ("SERVE", "README")]
p = []
a = p.append
a(f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="400" viewBox="0 0 {W} 400" font-family="Segoe UI, Ubuntu, Helvetica, sans-serif">')
a(f"""<style>
.t{{fill:{TXT}}} .m{{fill:{MUTED}}} .c{{fill:{TEAL}}}
.fade{{opacity:0;animation:fade .8s ease forwards}}
@keyframes fade{{to{{opacity:1}}}}
.pulse{{animation:pulse 1.6s ease-in-out infinite}}
@keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.25}}}}
.dash{{stroke-dasharray:6 6;animation:dash 1s linear infinite}}
@keyframes dash{{to{{stroke-dashoffset:-12}}}}
.cursor{{animation:pulse 1s steps(1) infinite}}
</style>""")
a(f'<rect width="{W}" height="400" rx="14" fill="{BG}" stroke="{LINE}"/>')
a(f'<text x="28" y="38" font-size="18" font-weight="700" class="t">$ python etl_github.py --user {USER}<tspan class="c cursor">▌</tspan></text>')
a(f'<circle cx="{W-230}" cy="32" r="5" fill="#2ECC71" class="pulse"/>')
a(f'<text x="{W-218}" y="37" font-size="12" class="m">pipeline OK · {hoje}</text>')

# nós e fluxo
xs = [100 + i * 217 for i in range(4)]
y = 100
a(f'<path id="flow" d="M{xs[0]} {y} L{xs[-1]} {y}" stroke="{LINE}" stroke-width="2" fill="none" class="dash"/>')
for i in range(6):  # pacotes de dados correndo
    cor = TEAL if i % 2 == 0 else "#50ABF1"
    a(f'<rect width="10" height="10" rx="2" fill="{cor}" x="-5" y="-5"><animateMotion dur="4s" begin="{i*0.66:.2f}s" repeatCount="indefinite"><mpath href="#flow"/></animateMotion></rect>')
for i, (x, (nome, sub)) in enumerate(zip(xs, etapas)):
    a(f'<g class="fade" style="animation-delay:{i*0.25}s">')
    a(f'<rect x="{x-72}" y="{y-30}" width="144" height="60" rx="10" fill="{CARD}" stroke="{TEAL}" stroke-width="1.5"/>')
    a(f'<text x="{x}" y="{y-4}" text-anchor="middle" font-size="14" font-weight="700" class="c">{nome}</text>')
    a(f'<text x="{x}" y="{y+16}" text-anchor="middle" font-size="12" class="m">{sub}</text></g>')

# KPIs (tabela fato)
a(f'<text x="28" y="182" font-size="13" font-weight="700" class="m">fato_github · {len(repos)} repositórios processados</text>')
for i, (rot, val) in enumerate(kpis):
    x = 28 + i * 201
    a(f'<g class="fade" style="animation-delay:{1+i*0.2:.1f}s">')
    a(f'<rect x="{x}" y="194" width="191" height="72" rx="10" fill="{CARD}"/>')
    a(f'<text x="{x+16}" y="232" font-size="28" font-weight="800" class="t">{val}</text>')
    a(f'<text x="{x+16}" y="254" font-size="12" class="m">{rot}</text></g>')

# barras de linguagem (dim_linguagem)
a(f'<text x="28" y="298" font-size="13" font-weight="700" class="m">dim_linguagem · top {len(top)} por nº de repositórios</text>')
for i, (lang, n) in enumerate(top):
    yy = 312 + i * 15
    w = 560 * n / top[0][1]
    cor = CORES.get(lang, TEAL)
    a(f'<text x="28" y="{yy+10}" font-size="11" class="t">{lang}</text>')
    a(f'<rect x="150" y="{yy+1}" width="0" height="10" rx="5" fill="{cor}"><animate attributeName="width" from="0" to="{w:.0f}" dur="1.2s" begin="{1.8+i*0.15:.2f}s" fill="freeze"/></rect>')
    a(f'<text x="{150+w+8:.0f}" y="{yy+10}" font-size="11" class="m fade" style="animation-delay:{3+i*0.15:.2f}s">{n} · {100*n/total_l:.0f}%</text>')
a("</svg>")

os.makedirs(os.path.dirname(OUT) or ".", exist_ok=True)
open(OUT, "w", encoding="utf-8").write("\n".join(p))
print(f"ok: {OUT} | kpis={kpis} | top={top} | último push={atualizado}")
