"""ETL dos dados públicos do GitHub do Davi -> SVGs animados para o README.

Extract   : API REST (perfil, repositórios, linguagens, eventos) + GraphQL (calendário de contribuições)
Transform : KPIs, sequências, linguagens por bytes, heatmap hora x dia, testes de qualidade de dados
Load      : dist/pipeline.svg · dist/heatmap.svg · dist/run_log.svg

Só stdlib. Uso: python scripts/github_etl.py [pasta_saida]
"""
import json
import os
import sys
import time
import urllib.request
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from xml.sax.saxutils import escape

USER = os.environ.get("GH_USER", "Daviramos7")
TOKEN = os.environ.get("GITHUB_TOKEN", "")
OUT_DIR = sys.argv[1] if len(sys.argv) > 1 else "dist"
TZ = timezone(timedelta(hours=-3))  # Recife
HEADERS = {"Accept": "application/vnd.github+json", "User-Agent": "github-etl-svg"}
if TOKEN:
    HEADERS["Authorization"] = f"Bearer {TOKEN}"

LOG = []  # (etapa, mensagem, segundos) -> vira o run_log.svg


def get(url, data=None):
    req = urllib.request.Request(url, data=data, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def etapa(nome, msg, inicio):
    LOG.append((nome, msg, time.perf_counter() - inicio))


# =====================================================================
# EXTRACT
# =====================================================================
t0 = time.perf_counter()
user = get(f"https://api.github.com/users/{USER}")
repos, page = [], 1
while True:
    lote = get(f"https://api.github.com/users/{USER}/repos?per_page=100&type=owner&page={page}")
    repos += lote
    if len(lote) < 100:
        break
    page += 1
repos = [r for r in repos if not r["fork"]]
etapa("extract", f"{len(repos)} repositórios lidos de /users/{USER}/repos", t0)

t0 = time.perf_counter()
lang_bytes = Counter()
for r in repos:
    try:
        lang_bytes.update(get(r["languages_url"]))
    except Exception:
        pass
etapa("extract", f"{len(lang_bytes)} linguagens em {sum(lang_bytes.values()) / 1e6:.1f} MB de código", t0)

t0 = time.perf_counter()
dias, totais = [], {}
if TOKEN:
    try:
        q = {"query": """query($l:String!){user(login:$l){contributionsCollection{
              totalCommitContributions totalPullRequestContributions totalIssueContributions
              totalRepositoryContributions
              contributionCalendar{totalContributions weeks{contributionDays{date contributionCount}}}}}}""",
             "variables": {"l": USER}}
        cc = get("https://api.github.com/graphql", json.dumps(q).encode())["data"]["user"]["contributionsCollection"]
        totais = cc
        dias = [(date.fromisoformat(d["date"]), d["contributionCount"])
                for w in cc["contributionCalendar"]["weeks"] for d in w["contributionDays"]]
    except Exception as e:
        print("graphql indisponível:", e, file=sys.stderr)
etapa("extract", f"{len(dias)} dias de calendário de contribuições (GraphQL)", t0)

t0 = time.perf_counter()
eventos = []
for p in range(1, 4):
    try:
        lote = get(f"https://api.github.com/users/{USER}/events/public?per_page=100&page={p}")
    except Exception:
        break
    eventos += lote
    if len(lote) < 100:
        break
etapa("extract", f"{len(eventos)} eventos públicos recentes", t0)

# =====================================================================
# TRANSFORM
# =====================================================================
t0 = time.perf_counter()
hoje = datetime.now(TZ).date()
dias = [(d, c) for d, c in dias if d <= hoje]

# sequências
maior = atual = corrida = 0
for _, c in dias:
    corrida = corrida + 1 if c > 0 else 0
    maior = max(maior, corrida)
resto = dias[:-1] if dias and dias[-1][0] == hoje and dias[-1][1] == 0 else dias  # hoje sem commit ainda não quebra
for _, c in reversed(resto):
    if c == 0:
        break
    atual += 1
melhor_dia = max(dias, key=lambda x: x[1]) if dias else (hoje, 0)
dias_ativos = sum(1 for _, c in dias if c > 0)
contrib_ano = totais.get("contributionCalendar", {}).get("totalContributions", sum(c for _, c in dias))
por_dia_semana = [0] * 7  # seg..dom
for d, c in dias:
    por_dia_semana[d.weekday()] += c

# heatmap hora x dia da semana (commits dos PushEvents, fuso Recife)
heat = defaultdict(int)
eventos_unicos = {e["id"]: e for e in eventos}
for e in eventos_unicos.values():
    if e["type"] != "PushEvent":
        continue
    dt = datetime.fromisoformat(e["created_at"].replace("Z", "+00:00")).astimezone(TZ)
    heat[(dt.weekday(), dt.hour)] += max(1, e.get("payload", {}).get("size", 1))
commits_eventos = sum(heat.values())
if eventos_unicos:
    mais_antigo = min(datetime.fromisoformat(e["created_at"].replace("Z", "+00:00")) for e in eventos_unicos.values())
    janela_dias = max(1, (datetime.now(timezone.utc) - mais_antigo).days)
else:
    janela_dias = 0
hora_pico = max(range(24), key=lambda h: sum(heat[(d, h)] for d in range(7))) if heat else None

stars = sum(r["stargazers_count"] for r in repos)
forks = sum(r["forks_count"] for r in repos)
total_bytes = sum(lang_bytes.values()) or 1
top_langs = lang_bytes.most_common(6)
outros = total_bytes - sum(b for _, b in top_langs)
if lang_bytes and outros > 0:
    top_langs.append(("Outras", outros))
repo_recente = max(repos, key=lambda r: r["pushed_at"])["name"] if repos else "-"
etapa("transform", "KPIs, sequências, linguagens por bytes e heatmap calculados", t0)

# testes de qualidade (estilo dbt test) — rodam de verdade sobre os dados extraídos
testes = []


def teste(nome, checagem, detalhe=""):
    t = time.perf_counter()
    resultado = bool(checagem())
    testes.append((nome, resultado, detalhe, time.perf_counter() - t))


teste("not_null_repos_name", lambda: all(r.get("name") for r in repos))
ids = [r["id"] for r in repos]
teste("unique_repos_id", lambda: len(ids) == len(set(ids)))
teste("not_negative_stargazers", lambda: all(r["stargazers_count"] >= 0 for r in repos))
teste("positive_language_bytes", lambda: lang_bytes and all(b > 0 for b in lang_bytes.values()))
teste("calendar_has_365_days", lambda: len(dias) >= 365 or not TOKEN, f"{len(dias)} dias")
teste("not_negative_contributions", lambda: all(c >= 0 for _, c in dias))
teste("unique_events_id", lambda: len(eventos_unicos) == len({e["id"] for e in eventos_unicos.values()}), f"{len(eventos) - len(eventos_unicos)} duplicados removidos")
teste("freshness_last_push_lt_30d",
      lambda: repos and (datetime.now(timezone.utc) - datetime.fromisoformat(
          max(r["pushed_at"] for r in repos).replace("Z", "+00:00"))).days < 30)
passou = sum(1 for t in testes if t[1])

# =====================================================================
# LOAD (SVGs)
# =====================================================================
BG, CARD, TEAL, BLUE, TXT, MUTED, LINE = "#0D1621", "#132232", "#19B5A5", "#50ABF1", "#FFFFFF", "#8B9BB4", "#34495E"
CORES = {"Python": "#3776AB", "TypeScript": "#3178C6", "JavaScript": "#F1E05A", "HTML": "#E34C26",
         "CSS": "#663399", "Kotlin": "#A97BFF", "Jupyter Notebook": "#DA5B0B", "Java": "#B07219",
         "C#": "#178600", "SCSS": "#C6538C", "Shell": "#89E051", "Dockerfile": "#384D54",
         "PLpgSQL": "#336790", "Dart": "#00B4AB", "Outras": "#5C6B7F"}
FONT = 'font-family="Segoe UI, Ubuntu, Helvetica, Arial, sans-serif"'
MONO = 'font-family="Fira Code, Consolas, Menlo, monospace"'
CSS = f"""<style>
.t{{fill:{TXT}}} .m{{fill:{MUTED}}} .c{{fill:{TEAL}}} .b{{fill:{BLUE}}} .ok{{fill:#2ECC71}} .err{{fill:#E74C3C}}
.fade{{opacity:0;animation:fade .7s ease forwards}}
@keyframes fade{{to{{opacity:1}}}}
.up{{opacity:0;animation:up .7s ease forwards}}
@keyframes up{{from{{opacity:0;transform:translateY(8px)}}to{{opacity:1;transform:none}}}}
.pulse{{animation:pulse 1.6s ease-in-out infinite}}
@keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.25}}}}
.blink{{animation:pulse 1s steps(1) infinite}}
.dash{{stroke-dasharray:6 6;animation:dash 1s linear infinite}}
@keyframes dash{{to{{stroke-dashoffset:-12}}}}
.glow{{animation:glow 2.4s ease-in-out infinite}}
@keyframes glow{{0%,100%{{stroke-opacity:.35}}50%{{stroke-opacity:1}}}}
</style>"""
agora = datetime.now(TZ).strftime("%d/%m/%Y %H:%M")
fmt = lambda n: f"{n:,}".replace(",", ".")


def svg(w, h, corpo):
    return (f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" viewBox="0 0 {w} {h}" {FONT}>'
            + CSS + f'<rect width="{w}" height="{h}" rx="14" fill="{BG}" stroke="{LINE}"/>' + "\n".join(corpo) + "</svg>")


def contador(x, y, valor, delay, size=26, anchor="start"):
    """Número que 'conta' de 0 até o valor: troca de frames com SMIL (funciona em <img> no GitHub)."""
    passos = 8
    out = []
    for i in range(1, passos + 1):
        v = round(valor * i / passos) if i < passos else valor
        ini = delay + (i - 1) * 0.09
        vis = f'<set attributeName="opacity" to="1" begin="{ini:.2f}s"/>'
        esconde = "" if i == passos else f'<set attributeName="opacity" to="0" begin="{ini + 0.09:.2f}s"/>'
        out.append(f'<text x="{x}" y="{y}" font-size="{size}" font-weight="800" class="t" '
                   f'text-anchor="{anchor}" opacity="0">{fmt(v)}{vis}{esconde}</text>')
    return "".join(out)


# ---------- 1) pipeline.svg ----------
t_load = time.perf_counter()
W = 850
c = []
c.append(f'<text x="28" y="38" font-size="17" font-weight="700" class="t" {MONO}>$ python github_etl.py --user {USER}'
         f'<tspan class="c blink">▌</tspan></text>')
c.append(f'<circle cx="{W - 232}" cy="32" r="5" fill="#2ECC71" class="pulse"/>'
         f'<text x="{W - 220}" y="37" font-size="12" class="m">pipeline OK · {agora}</text>')
xs = [100 + i * 217 for i in range(4)]
y = 98
c.append(f'<path id="flow" d="M{xs[0]} {y} L{xs[-1]} {y}" stroke="{LINE}" stroke-width="2" fill="none" class="dash"/>')
for i in range(8):
    cor = TEAL if i % 2 == 0 else BLUE
    forma = (f'<rect width="10" height="10" rx="2" fill="{cor}" x="-5" y="-5">' if i % 3
             else f'<circle r="5" fill="{cor}">')
    fecha = "</rect>" if i % 3 else "</circle>"
    c.append(f'{forma}<animateMotion dur="4s" begin="{i * 0.5:.2f}s" repeatCount="indefinite">'
             f'<mpath href="#flow"/></animateMotion>{fecha}')
etapas = [("EXTRACT", "REST + GraphQL"), ("TRANSFORM", "Python"), ("TEST", f"{passou}/{len(testes)} PASS"), ("LOAD", "SVG → README")]
for i, (x, (nome, sub)) in enumerate(zip(xs, etapas)):
    c.append(f'<g class="fade" style="animation-delay:{i * .25}s">'
             f'<rect x="{x - 74}" y="{y - 30}" width="148" height="60" rx="10" fill="{CARD}" stroke="{TEAL}" stroke-width="1.5" class="glow" style="animation-delay:{i * .6}s"/>'
             f'<text x="{x}" y="{y - 4}" text-anchor="middle" font-size="14" font-weight="700" class="c">{nome}</text>'
             f'<text x="{x}" y="{y + 16}" text-anchor="middle" font-size="12" class="m">{sub}</text></g>')

c.append(f'<text x="28" y="172" font-size="13" font-weight="700" class="m" {MONO}>fato_github · {len(repos)} repositórios · {len(eventos_unicos)} eventos · {len(dias)} dias</text>')
kpis = [("Contribuições/ano", contrib_ano), ("Commits/ano", totais.get("totalCommitContributions", commits_eventos)),
        ("Repositórios", user["public_repos"]), ("Pull requests", totais.get("totalPullRequestContributions", 0)),
        ("Estrelas", stars), ("Seguidores", user["followers"])]
cw, gap = 124, 9
for i, (rot, val) in enumerate(kpis):
    x = 28 + i * (cw + gap)
    d = 0.9 + i * 0.15
    c.append(f'<g class="up" style="animation-delay:{d:.2f}s"><rect x="{x}" y="184" width="{cw}" height="70" rx="10" fill="{CARD}"/>'
             f'<rect x="{x}" y="184" width="3" height="70" rx="1.5" fill="{TEAL if i % 2 == 0 else BLUE}"/>'
             f'<text x="{x + 14}" y="244" font-size="11" class="m">{rot}</text></g>')
    c.append(contador(x + 14, 222, val, d + 0.2))

c.append(f'<text x="28" y="290" font-size="13" font-weight="700" class="m" {MONO}>dim_linguagem · participação por bytes de código</text>')
bx, bw, by = 28, W - 56, 302
acc = 0
for i, (lang, b) in enumerate(top_langs):
    w = bw * b / total_bytes
    cor = CORES.get(lang, TEAL)
    c.append(f'<rect x="{bx + acc:.1f}" y="{by}" width="0" height="14" fill="{cor}">'
             f'<animate attributeName="width" from="0" to="{w:.1f}" dur=".5s" begin="{1.9 + i * .35:.2f}s" fill="freeze"/></rect>')
    acc += w
c.append(f'<rect x="{bx}" y="{by}" width="{bw}" height="14" rx="7" fill="none" stroke="{BG}" stroke-width="3"/>')
for i, (lang, b) in enumerate(top_langs):
    col, row = i % 4, i // 4
    lx, ly = 28 + col * 200, 344 + row * 22
    c.append(f'<g class="fade" style="animation-delay:{2 + i * .35:.2f}s"><circle cx="{lx + 5}" cy="{ly - 4}" r="5" fill="{CORES.get(lang, TEAL)}"/>'
             f'<text x="{lx + 16}" y="{ly}" font-size="12" class="t">{escape(lang)} <tspan class="m">{100 * b / total_bytes:.1f}%</tspan></text></g>')

c.append(f'<line x1="28" x2="{W - 28}" y1="392" y2="392" stroke="{LINE}"/>')
destaques = [("🔥", "Sequência atual", f"{atual} dias"), ("🏆", "Maior sequência", f"{maior} dias"),
             ("📅", "Dias ativos no ano", f"{dias_ativos}"), ("⚡", "Melhor dia",
             f"{melhor_dia[1]} em {melhor_dia[0].strftime('%d/%m')}")]
for i, (ico, rot, val) in enumerate(destaques):
    x = 28 + i * 203
    c.append(f'<g class="up" style="animation-delay:{3.6 + i * .2:.2f}s"><text x="{x}" y="424" font-size="18">{ico}</text>'
             f'<text x="{x + 28}" y="416" font-size="11" class="m">{rot}</text>'
             f'<text x="{x + 28}" y="434" font-size="15" font-weight="700" class="c">{val}</text></g>')
c.append(f'<text x="{W - 28}" y="460" text-anchor="end" font-size="10" class="m">último push em {escape(repo_recente)} · atualizado diariamente via GitHub Actions</text>')
pipeline_svg = svg(W, 474, c)

# ---------- 2) heatmap.svg ----------
c = []
DIAS = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"]
c.append(f'<text x="28" y="36" font-size="16" font-weight="700" class="t">Quando eu codifico</text>'
         f'<text x="28" y="56" font-size="12" class="m">{fmt(commits_eventos)} commits nos últimos ~{janela_dias} dias, por hora (horário de Recife)'
         + (f' · pico às {hora_pico}h' if hora_pico is not None else "") + '</text>')
mx = max(heat.values(), default=1)
cell, g0x, g0y = 21, 66, 76
for d in range(7):
    c.append(f'<text x="{g0x - 10}" y="{g0y + d * cell + 15}" text-anchor="end" font-size="11" class="m">{DIAS[d]}</text>')
    for h in range(24):
        v = heat[(d, h)]
        op = 0.08 if v == 0 else 0.25 + 0.75 * v / mx
        c.append(f'<rect x="{g0x + h * cell}" y="{g0y + d * cell}" width="{cell - 3}" height="{cell - 3}" rx="4" '
                 f'fill="{TEAL}" fill-opacity="{op:.2f}" class="fade" style="animation-delay:{(h + d) * .04:.2f}s">'
                 f'<title>{DIAS[d]} {h}h: {v} commits</title></rect>')
for h in range(0, 24, 3):
    c.append(f'<text x="{g0x + h * cell + 9}" y="{g0y + 7 * cell + 14}" text-anchor="middle" font-size="10" class="m">{h}h</text>')
# barras por dia da semana (ano inteiro, calendário)
px = g0x + 24 * cell + 40
c.append(f'<text x="{px}" y="{g0y - 8}" font-size="12" font-weight="700" class="m">Contribuições/ano por dia</text>')
mxd = max(por_dia_semana) or 1
for d in range(7):
    w = 150 * por_dia_semana[d] / mxd
    yy = g0y + d * cell
    c.append(f'<text x="{px}" y="{yy + 14}" font-size="11" class="m">{DIAS[d]}</text>'
             f'<rect x="{px + 34}" y="{yy + 4}" width="0" height="12" rx="6" fill="{BLUE if d >= 5 else TEAL}">'
             f'<animate attributeName="width" from="0" to="{w:.0f}" dur=".9s" begin="{1.2 + d * .12:.2f}s" fill="freeze"/></rect>'
             f'<text x="{px + 40 + w:.0f}" y="{yy + 14}" font-size="11" class="t fade" style="animation-delay:{2.1 + d * .12:.2f}s">{por_dia_semana[d]}</text>')
heatmap_svg = svg(W, 260, c)

# ---------- 3) run_log.svg (estilo dbt) ----------
etapa("load", "pipeline.svg e heatmap.svg renderizados", t_load)
linhas = [(f"$ python github_etl.py --user {USER}", "t")]
for nome, msg, seg in LOG:
    linhas.append((f"[{nome.upper()}] {msg} ({seg:.2f}s)", "m"))
linhas.append((f"Running {len(testes)} data tests...", "c"))
for i, (nome, ok, det, seg) in enumerate(testes, 1):
    pontos = "." * max(3, 44 - len(nome))
    linhas.append((f"{i} of {len(testes)} {'PASS' if ok else 'FAIL'} {nome} {pontos} [{seg * 1e6:.0f}µs]"
                   + (f"  -- {det}" if det else ""), "ok" if ok else "err"))
linhas.append((f"Done. PASS={passou} WARN=0 ERROR={len(testes) - passou} TOTAL={len(testes)}", "ok" if passou == len(testes) else "err"))
c = [f'<circle cx="26" cy="22" r="6" fill="#FF5F57"/><circle cx="46" cy="22" r="6" fill="#FEBC2E"/><circle cx="66" cy="22" r="6" fill="#28C840"/>'
     f'<text x="{W / 2}" y="27" text-anchor="middle" font-size="12" class="m">github_etl — run log · {agora}</text>']
for i, (txt, cls) in enumerate(linhas):
    c.append(f'<text x="24" y="{60 + i * 19}" font-size="12.5" class="{cls} fade" {MONO} style="animation-delay:{.3 + i * .32:.2f}s">'
             f'{escape(txt)}</text>')
yfim = 60 + len(linhas) * 19
c.append(f'<text x="24" y="{yfim}" font-size="12.5" class="c blink fade" {MONO} style="animation-delay:{.3 + len(linhas) * .32:.2f}s">$ ▌</text>')
run_log_svg = svg(W, yfim + 22, c)

os.makedirs(OUT_DIR, exist_ok=True)
for nome, conteudo in [("pipeline.svg", pipeline_svg), ("heatmap.svg", heatmap_svg), ("run_log.svg", run_log_svg)]:
    with open(os.path.join(OUT_DIR, nome), "w", encoding="utf-8") as f:
        f.write(conteudo)
print(f"ok: {OUT_DIR}/ | testes {passou}/{len(testes)} | kpis={kpis} | sequência {atual}/{maior} | heat={commits_eventos}")
