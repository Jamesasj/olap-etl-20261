"""
Segunda execução de inserção de dados.
Gera novos registros mantendo integridade referencial com os dados de inserts_001*.sql.

Volumes:
  ATOR       = 0  (usa os 50 existentes)
  CLIENTE    = 5  (novos: 101-105)
  FILME      = 2  (novos: 81-82)
  EMPRESTIMO = até 500 (limitado pelas mídias disponíveis sem empréstimo)

Lê inserts_001b.sql e inserts_001c.sql para descobrir mídias existentes e
empréstimos já alocados, gerando empréstimos apenas para mídias livres.
"""

import random
import os
import re
import glob
from datetime import date, timedelta

# ============================================================
# Configurações da execução 2
# ============================================================

QTDE_CLIENTE = 5
QTDE_FILME = 2
QTDE_EMPRESTIMO = 500

# Offsets (baseados no que exec1 gerou)
OFFSET_CLIENTE = 100   # novos: 101..105
OFFSET_FILME = 80      # novos: 81..82

# Dados existentes que serão referenciados
ATORES_EXISTENTES = list(range(1, 51))    # cod 1..50
CLIENTES_EXISTENTES = list(range(1, 101))  # num_Cliente 1..100

CLASSIFICACOES = [
    (1, 'Livre', 5.00),
    (2, '10 anos', 6.00),
    (3, '12 anos', 7.00),
    (4, '16 anos', 8.50),
    (5, '18 anos', 10.00),
]

# ============================================================
# Pools de dados (mesmo do generate-values.py)
# ============================================================

NOMES_BR = [
    'Ana', 'Bruno', 'Carlos', 'Daniela', 'Eduardo', 'Fernanda', 'Gabriel',
    'Helena', 'Igor', 'Julia', 'Leonardo', 'Mariana', 'Nicolas', 'Olivia',
    'Pedro', 'Rafaela', 'Samuel', 'Tatiana', 'Vinicius', 'Beatriz',
    'Lucas', 'Camila', 'Matheus', 'Larissa', 'Felipe', 'Amanda',
    'Rodrigo', 'Patricia', 'Thiago', 'Renata',
]

SOBRENOMES_BR = [
    'Silva', 'Santos', 'Oliveira', 'Souza', 'Rodrigues', 'Ferreira',
    'Almeida', 'Nascimento', 'Lima', 'Araujo', 'Pereira', 'Barbosa',
    'Ribeiro', 'Carvalho', 'Gomes', 'Martins', 'Rocha', 'Costa',
    'Mendes', 'Cardoso',
]

NOMES_INTER = [
    'John', 'Emma', 'James', 'Sophie', 'Robert', 'Marie', 'William',
    'Isabella', 'Michael', 'Charlotte', 'David', 'Amelia', 'Richard',
    'Olivia', 'Thomas', 'Sophia', 'Daniel', 'Mia', 'Joseph', 'Emily',
    'Hans', 'Yuki', 'Pierre', 'Giulia', 'Carlos', 'Ingrid', 'Ahmed',
    'Priya', 'Kenji', 'Lena',
]

SOBRENOMES_INTER = [
    'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller',
    'Davis', 'Rodriguez', 'Martinez', 'Anderson', 'Taylor', 'Thomas',
    'Moore', 'Jackson', 'Martin', 'Lee', 'Thompson', 'White', 'Harris',
    'Muller', 'Tanaka', 'Dubois', 'Rossi', 'Johansson',
]

RUAS = [
    'Rua das Flores', 'Av. Brasil', 'Rua Sao Paulo', 'Av. Paulista',
    'Rua XV de Novembro', 'Rua Santos Dumont', 'Av. Rio Branco',
    'Rua Tiradentes', 'Rua Marechal Deodoro', 'Av. Independencia',
    'Rua Barao do Rio Branco', 'Rua Dom Pedro II', 'Av. Pres. Vargas',
    'Rua Vol. da Patria', 'Rua Major Facundo',
]

DDDS = ['11', '21', '31', '41', '51', '61', '71', '81', '85', '91']

CATEGORIAS_FILME = [
    'Acao', 'Comedia', 'Drama', 'Terror', 'Ficcao Cientifica',
    'Romance', 'Animacao', 'Suspense', 'Documentario',
]

ADJ_EN = [
    'Dark', 'Last', 'Lost', 'Final', 'Silent', 'Hidden', 'Broken',
    'Eternal', 'Crimson', 'Golden', 'Iron', 'Crystal', 'Shadow',
    'Frozen', 'Wild', 'Savage', 'Mystic', 'Ancient', 'Burning', 'Steel',
]

NOUN_EN = [
    'Night', 'Storm', 'Dream', 'World', 'Heart', 'Road', 'Sky',
    'Fire', 'Ocean', 'Mountain', 'River', 'Forest', 'City', 'Kingdom',
    'Empire', 'Legend', 'Secret', 'Journey', 'Destiny', 'Dawn',
]

PREF_PT = ['O', 'A', 'Os', 'As']

NOUN_PT = [
    'Noite', 'Tempestade', 'Sonho', 'Mundo', 'Coracao', 'Caminho',
    'Ceu', 'Fogo', 'Oceano', 'Montanha', 'Rio', 'Floresta', 'Cidade',
    'Reino', 'Imperio', 'Lenda', 'Segredo', 'Jornada', 'Destino', 'Aurora',
]

ADJ_PT = [
    'Sombrio', 'Ultimo', 'Perdido', 'Final', 'Silencioso', 'Oculto',
    'Quebrado', 'Eterno', 'Carmesim', 'Dourado', 'de Ferro', 'Cristal',
    'das Sombras', 'Gelado', 'Selvagem', 'Mistico', 'Antigo', 'Ardente',
]


# ============================================================
# Funções auxiliares
# ============================================================

def esc(text):
    return text.replace("'", "''")


def gerar_nome_br():
    return f"{random.choice(NOMES_BR)} {random.choice(SOBRENOMES_BR)}"


def gerar_nome_inter():
    return f"{random.choice(NOMES_INTER)} {random.choice(SOBRENOMES_INTER)}"


def gerar_telefone_fixo():
    ddd = random.choice(DDDS)
    n = f"{random.randint(2000, 5999)}-{random.randint(1000, 9999)}"
    return f"({ddd}) {n}"


def gerar_telefone_cel():
    ddd = random.choice(DDDS)
    n = f"9{random.randint(1000, 9999)}-{random.randint(1000, 9999)}"
    return f"({ddd}) {n}"


def gerar_endereco():
    return f"{random.choice(RUAS)}, {random.randint(1, 3000)}"


def gerar_data(ano_ini, ano_fim):
    inicio = date(ano_ini, 1, 1)
    fim = date(ano_fim, 12, 31)
    return inicio + timedelta(days=random.randint(0, (fim - inicio).days))


def gerar_titulo_en():
    padrao = random.choice([
        lambda: f"The {random.choice(ADJ_EN)} {random.choice(NOUN_EN)}",
        lambda: f"{random.choice(NOUN_EN)} of {random.choice(NOUN_EN)}",
        lambda: f"{random.choice(ADJ_EN)} {random.choice(NOUN_EN)}",
        lambda: f"The {random.choice(NOUN_EN)}",
    ])
    return padrao()


def gerar_titulo_pt():
    padrao = random.choice([
        lambda: f"{random.choice(PREF_PT)} {random.choice(NOUN_PT)} {random.choice(ADJ_PT)}",
        lambda: f"{random.choice(NOUN_PT)} de {random.choice(NOUN_PT)}",
        lambda: f"{random.choice(PREF_PT)} {random.choice(ADJ_PT)} {random.choice(NOUN_PT)}",
        lambda: f"{random.choice(PREF_PT)} {random.choice(NOUN_PT)}",
    ])
    return padrao()


# ============================================================
# Leitura dos dados existentes (inserts_001*.sql)
# ============================================================

def parse_midias_existentes(caminho):
    """Extrai (numFilme, numero, tipo) dos INSERTs de MIDIA."""
    midias = {}
    padrao = re.compile(
        r"INSERT INTO MIDIA.*VALUES\s*\((\d+),\s*(\d+),\s*'([^']+)'\)"
    )
    with open(caminho, 'r', encoding='utf-8') as f:
        for linha in f:
            m = padrao.search(linha)
            if m:
                nf, num, tipo = int(m.group(1)), int(m.group(2)), m.group(3)
                midias[(nf, num)] = tipo
    return midias


def parse_emprestimos_existentes(caminho):
    """Extrai PKs (numFilme, numero) dos INSERTs de EMPRESTIMO."""
    pks = set()
    padrao = re.compile(
        r"INSERT INTO EMPRESTIMO.*VALUES\s*\((\d+),\s*(\d+),"
    )
    with open(caminho, 'r', encoding='utf-8') as f:
        for linha in f:
            m = padrao.search(linha)
            if m:
                pks.add((int(m.group(1)), int(m.group(2))))
    return pks


def parse_filmes_classificacao(caminho):
    """Extrai {numFilme: classificacao} dos INSERTs de FILME."""
    mapa = {}
    padrao = re.compile(
        r"INSERT INTO FILME.*VALUES\s*\((\d+),.*,\s*(\d+)\)\s*;"
    )
    with open(caminho, 'r', encoding='utf-8') as f:
        for linha in f:
            m = padrao.search(linha)
            if m:
                mapa[int(m.group(1))] = int(m.group(2))
    return mapa


# ============================================================
# Geração de dados
# ============================================================

def gerar_clientes(qtde, offset):
    clientes = []
    for i in range(1, qtde + 1):
        num = offset + i
        nome = gerar_nome_br()
        end = gerar_endereco()
        fres = gerar_telefone_fixo()
        fcel = gerar_telefone_cel()
        clientes.append((num, nome, end, fres, fcel))
    return clientes


def gerar_filmes(qtde, offset, classificacoes):
    filmes = []
    titulos_usados = set()
    for i in range(1, qtde + 1):
        num = offset + i
        while True:
            t_en = gerar_titulo_en()
            if t_en not in titulos_usados:
                titulos_usados.add(t_en)
                break
        t_pt = gerar_titulo_pt()
        duracao = max(80, min(180, int(random.gauss(120, 25))))
        dt_lanc = gerar_data(1990, 2025)
        direcao = gerar_nome_inter()
        categ = random.choice(CATEGORIAS_FILME)
        classif = random.choice(classificacoes)[0]
        filmes.append((num, t_en, t_pt, duracao, dt_lanc, direcao, categ, classif))
    return filmes


def gerar_estrelas(filmes, atores_cods):
    estrelas = []
    for filme in filmes:
        nf = filme[0]
        k = random.randint(2, min(5, len(atores_cods)))
        for ca in random.sample(atores_cods, k):
            estrelas.append((nf, ca))
    return estrelas


def gerar_emprestimos(qtde, midias_disponiveis, todos_clientes, filme_classif_map):
    """
    Gera empréstimos apenas para mídias que ainda não têm empréstimo.
    midias_disponiveis: dict {(numFilme, numero): tipo} apenas das mídias livres.
    """
    preco_map = {c[0]: c[2] for c in CLASSIFICACOES}
    lista_midias = list(midias_disponiveis.keys())

    if not lista_midias:
        print("  AVISO: Nenhuma midia disponivel para emprestimo!")
        return []

    efetivo = min(qtde, len(lista_midias))
    if efetivo < qtde:
        print(f"  AVISO: Solicitados {qtde} emprestimos, mas apenas {efetivo} midias disponiveis.")
        print(f"         PK de EMPRESTIMO = PK de MIDIA -> max 1 emprestimo por midia.")

    # Embaralha e pega as mídias disponíveis
    random.shuffle(lista_midias)
    selecionadas = lista_midias[:efetivo]

    todos_cli_ids = list(todos_clientes)
    emprestimos = []

    for nf, num in selecionadas:
        tipo = midias_disponiveis[(nf, num)]
        cli = random.choice(todos_cli_ids)

        dt_emt = gerar_data(2023, 2026)
        if random.random() < 0.4:
            ds = dt_emt.weekday()
            if ds < 5:
                dt_emt += timedelta(days=(5 - ds))

        if random.random() < 0.1:
            dt_dev = None
        else:
            dt_dev = dt_emt + timedelta(days=random.randint(1, 14))

        classif_cod = filme_classif_map.get(nf, 1)
        preco_base = preco_map.get(classif_cod, 5.00)
        if dt_dev and (dt_dev - dt_emt).days > 7:
            extras = (dt_dev - dt_emt).days - 7
            valor = round(preco_base + extras * 1.50, 2)
        else:
            valor = preco_base

        emprestimos.append((nf, num, tipo, cli, dt_emt, dt_dev, valor))

    return emprestimos


# ============================================================
# Formatação SQL
# ============================================================

def sql_cliente(r):
    return (f"INSERT INTO CLIENTE (num_Cliente, nome, endereco, foneRes, foneCel) "
            f"VALUES ({r[0]}, '{esc(r[1])}', '{esc(r[2])}', '{esc(r[3])}', '{esc(r[4])}');")


def sql_filme(r):
    return (f"INSERT INTO FILME (numFilme, titulo_original, titulo_pt, duracao, "
            f"data_lancamento, direcao, categoria, classificacao) "
            f"VALUES ({r[0]}, '{esc(r[1])}', '{esc(r[2])}', {r[3]}, "
            f"'{r[4]}', '{esc(r[5])}', '{esc(r[6])}', {r[7]});")


def sql_estrela(r):
    return f"INSERT INTO ESTRELA (num_Filme, codAtor) VALUES ({r[0]}, {r[1]});"


def sql_emprestimo(r):
    dev = f"'{r[5]}'" if r[5] else "NULL"
    return (f"INSERT INTO EMPRESTIMO (numFilme, numero, tipo, cliente, dataEmt, dateDev, valor_pg) "
            f"VALUES ({r[0]}, {r[1]}, '{esc(r[2])}', {r[3]}, '{r[4]}', {dev}, {r[6]:.2f});")


# ============================================================
# Escrita dos arquivos
# ============================================================

def escrever_arquivo(caminho, secoes):
    total = 0
    with open(caminho, 'w', encoding='utf-8') as f:
        for nome, linhas in secoes:
            f.write(f"-- {nome}\n")
            for linha in linhas:
                f.write(linha + "\n")
            f.write("\n")
            total += len(linhas)
    print(f"  {os.path.basename(caminho)} -> {total} inserts")


def proximo_numero(diretorio):
    existentes = glob.glob(os.path.join(diretorio, "inserts_*a.sql"))
    if not existentes:
        return 1
    numeros = []
    for f in existentes:
        m = re.search(r'inserts_(\d+)a\.sql', os.path.basename(f))
        if m:
            numeros.append(int(m.group(1)))
    return max(numeros) + 1 if numeros else 1


# ============================================================
# Main
# ============================================================

def main():
    diretorio = os.path.dirname(os.path.abspath(__file__))

    # Verificar arquivos da exec1
    arq_b = os.path.join(diretorio, "inserts_001b.sql")
    arq_c = os.path.join(diretorio, "inserts_001c.sql")

    if not os.path.exists(arq_b) or not os.path.exists(arq_c):
        print("ERRO: Arquivos inserts_001b.sql e inserts_001c.sql nao encontrados.")
        print("      Execute generate-values.py primeiro.")
        return

    # Ler dados existentes
    print("Lendo dados existentes de inserts_001...")
    midias_existentes = parse_midias_existentes(arq_b)
    emprestimos_pks = parse_emprestimos_existentes(arq_c)
    filmes_classif = parse_filmes_classificacao(arq_b)

    print(f"  Midias existentes: {len(midias_existentes)}")
    print(f"  Emprestimos existentes (PKs unicas): {len(emprestimos_pks)}")

    # Determinar próximo número de arquivo
    num = proximo_numero(diretorio)
    prefixo = f"inserts_{num:03d}"
    print(f"\nExecucao #{num:03d}")
    print("Gerando dados...")

    # Gerar novos dados com offsets
    clientes = gerar_clientes(QTDE_CLIENTE, OFFSET_CLIENTE)
    filmes = gerar_filmes(QTDE_FILME, OFFSET_FILME, CLASSIFICACOES)
    estrelas = gerar_estrelas(filmes, ATORES_EXISTENTES)

    # Mapa de classificações para todos os filmes (existentes + novos)
    filme_classif_map = dict(filmes_classif)
    for f in filmes:
        filme_classif_map[f[0]] = f[7]

    # Calcular mídias disponíveis para empréstimo
    # = (mídias existentes sem empréstimo) + (mídias novas)
    midias_disponiveis = {}
    for pk, tipo in midias_existentes.items():
        if pk not in emprestimos_pks:
            midias_disponiveis[pk] = tipo

    print(f"  Midias disponiveis para emprestimo: {len(midias_disponiveis)}")

    # Todos os clientes (existentes + novos)
    todos_clientes = CLIENTES_EXISTENTES + [c[0] for c in clientes]

    # Gerar empréstimos
    emprestimos = gerar_emprestimos(
        QTDE_EMPRESTIMO, midias_disponiveis, todos_clientes, filme_classif_map
    )

    # Formatar SQL
    s_clientes = [sql_cliente(r) for r in clientes]
    s_filmes = [sql_filme(r) for r in filmes]
    s_estrelas = [sql_estrela(r) for r in estrelas]
    s_emprest = [sql_emprestimo(r) for r in emprestimos]

    # Arquivo a: CLIENTE (sem CLASSIFICACAO nem ATOR — já existem)
    escrever_arquivo(os.path.join(diretorio, f"{prefixo}a.sql"), [
        ("CLIENTE", s_clientes),
    ])

    # Arquivo b: FILME, ESTRELA
    escrever_arquivo(os.path.join(diretorio, f"{prefixo}b.sql"), [
        ("FILME", s_filmes),
        ("ESTRELA", s_estrelas),
    ])

    # Arquivo c: EMPRESTIMO
    escrever_arquivo(os.path.join(diretorio, f"{prefixo}c.sql"), [
        ("EMPRESTIMO", s_emprest),
    ])

    print("Concluido!")


if __name__ == "__main__":
    main()
