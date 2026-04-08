"""
Gera INSERT INTO para as tabelas definidas em ddl.sql (locadora de filmes).

Cada execução produz 3 arquivos SQL sequenciais (inserts_NNNa.sql, inserts_NNNb.sql, inserts_NNNc.sql)
com dados complementares e sem sobreposição de PKs.

Arquivo a: tabelas independentes (CLASSIFICACAO, ATOR, CLIENTE)
Arquivo b: tabelas dependentes (FILME, MIDIA, ESTRELA)
Arquivo c: tabela transacional (EMPRESTIMO)

As tabelas já devem estar criadas no banco. O script gera apenas os INSERTs.
"""

import random
import os
import glob
import re
from datetime import date, timedelta


# ============================================================
# Constantes e configurações
# ============================================================

QTDE_ATOR = 0
QTDE_CLIENTE = 5
QTDE_FILME = 2
QTDE_EMPRESTIMO = 500

CLASSIFICACOES = [
    (1, 'Livre', 5.00),
    (2, '10 anos', 6.00),
    (3, '12 anos', 7.00),
    (4, '16 anos', 8.50),
    (5, '18 anos', 10.00),
]

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

NACIONALIDADES = [
    'Brasileiro', 'Americano', 'Britanico', 'Frances', 'Italiano',
    'Alemao', 'Japones', 'Espanhol', 'Argentino', 'Mexicano',
    'Canadense', 'Australiano', 'Portugues', 'Sueco', 'Indiano',
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

TIPOS_MIDIA = ['DVD', 'Blu-ray', 'VHS']


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
# Geração de dados por tabela
# ============================================================

def gerar_classificacoes():
    return list(CLASSIFICACOES)


def gerar_atores(qtde):
    atores = []
    for cod in range(1, qtde + 1):
        dn = gerar_data(1950, 2000)
        nac = random.choice(NACIONALIDADES)
        real = gerar_nome_inter()
        artistico = gerar_nome_inter()
        atores.append((cod, dn, nac, real, artistico))
    return atores


def gerar_clientes(qtde):
    clientes = []
    for num in range(1, qtde + 1):
        nome = gerar_nome_br()
        end = gerar_endereco()
        fres = gerar_telefone_fixo()
        fcel = gerar_telefone_cel()
        clientes.append((num, nome, end, fres, fcel))
    return clientes


def gerar_filmes(qtde, classificacoes):
    filmes = []
    titulos_usados = set()
    for num in range(1, qtde + 1):
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


def gerar_midias(filmes):
    midias = []
    for filme in filmes:
        nf = filme[0]
        qtde = random.randint(1, 3)
        tipos = random.sample(TIPOS_MIDIA, qtde)
        for i, tipo in enumerate(tipos, 1):
            midias.append((nf, i, tipo))
    return midias


def gerar_estrelas(filmes, atores):
    estrelas = []
    cods = [a[0] for a in atores]
    for filme in filmes:
        nf = filme[0]
        k = random.randint(2, min(5, len(cods)))
        for ca in random.sample(cods, k):
            estrelas.append((nf, ca))
    return estrelas


def gerar_emprestimos(qtde, midias, clientes, filmes, classificacoes):
    preco_map = {c[0]: c[2] for c in classificacoes}
    filme_classif = {f[0]: f[7] for f in filmes}
    midia_tipo = {(m[0], m[1]): m[2] for m in midias}
    lista_midias = list(midia_tipo.keys())
    cods_cli = [c[0] for c in clientes]

    emprestimos = []
    for _ in range(qtde):
        nf, num = random.choice(lista_midias)
        tipo = midia_tipo[(nf, num)]
        cli = random.choice(cods_cli)

        dt_emt = gerar_data(2023, 2026)
        # 40% chance de cair em fim de semana
        if random.random() < 0.4:
            ds = dt_emt.weekday()
            if ds < 5:
                dt_emt += timedelta(days=(5 - ds))

        # 10% sem devolução
        if random.random() < 0.1:
            dt_dev = None
        else:
            dt_dev = dt_emt + timedelta(days=random.randint(1, 14))

        preco_base = preco_map[filme_classif[nf]]
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

def sql_classificacao(r):
    return f"INSERT INTO CLASSIFICACAO (cod, nome, preco) VALUES ({r[0]}, '{esc(r[1])}', {r[2]:.2f});"


def sql_ator(r):
    return (f"INSERT INTO ATOR (cod, dnascimento, nacionalidade, nomereal, nomeartistico) "
            f"VALUES ({r[0]}, '{r[1]}', '{esc(r[2])}', '{esc(r[3])}', '{esc(r[4])}');")


def sql_cliente(r):
    return (f"INSERT INTO CLIENTE (num_Cliente, nome, endereco, foneRes, foneCel) "
            f"VALUES ({r[0]}, '{esc(r[1])}', '{esc(r[2])}', '{esc(r[3])}', '{esc(r[4])}');")


def sql_filme(r):
    return (f"INSERT INTO FILME (numFilme, titulo_original, titulo_pt, duracao, "
            f"data_lancamento, direcao, categoria, classificacao) "
            f"VALUES ({r[0]}, '{esc(r[1])}', '{esc(r[2])}', {r[3]}, "
            f"'{r[4]}', '{esc(r[5])}', '{esc(r[6])}', {r[7]});")


def sql_midia(r):
    return f"INSERT INTO MIDIA (numFilme, numero, tipo) VALUES ({r[0]}, {r[1]}, '{esc(r[2])}');"


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


# ============================================================
# Main
# ============================================================

def main():
    diretorio = os.path.dirname(os.path.abspath(__file__))
    num = proximo_numero(diretorio)
    prefixo = f"inserts_{num:03d}"

    print(f"Execucao #{num:03d}")
    print("Gerando dados...")

    # Gerar todos os dados em memória
    classificacoes = gerar_classificacoes()
    atores = gerar_atores(QTDE_ATOR)
    clientes = gerar_clientes(QTDE_CLIENTE)
    filmes = gerar_filmes(QTDE_FILME, classificacoes)
    midias = gerar_midias(filmes)
    estrelas = gerar_estrelas(filmes, atores)
    emprestimos = gerar_emprestimos(QTDE_EMPRESTIMO, midias, clientes, filmes, classificacoes)

    # Formatar SQL
    s_classif = [sql_classificacao(r) for r in classificacoes]
    s_atores = [sql_ator(r) for r in atores]
    s_clientes = [sql_cliente(r) for r in clientes]
    s_filmes = [sql_filme(r) for r in filmes]
    s_midias = [sql_midia(r) for r in midias]
    s_estrelas = [sql_estrela(r) for r in estrelas]
    s_emprest = [sql_emprestimo(r) for r in emprestimos]

    # Arquivo a: tabelas independentes (CLASSIFICACAO, ATOR, CLIENTE)
    escrever_arquivo(os.path.join(diretorio, f"{prefixo}a.sql"), [
        ("CLASSIFICACAO", s_classif),
        ("ATOR", s_atores),
        ("CLIENTE", s_clientes),
    ])

    # Arquivo b: tabelas dependentes (FILME, MIDIA, ESTRELA)
    escrever_arquivo(os.path.join(diretorio, f"{prefixo}b.sql"), [
        ("FILME", s_filmes),
        ("MIDIA", s_midias),
        ("ESTRELA", s_estrelas),
    ])

    # Arquivo c: tabela transacional (EMPRESTIMO)
    escrever_arquivo(os.path.join(diretorio, f"{prefixo}c.sql"), [
        ("EMPRESTIMO", s_emprest),
    ])

    print("Concluido!")


if __name__ == "__main__":
    main()

