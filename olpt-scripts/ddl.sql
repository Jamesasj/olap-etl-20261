-- Tabelas independentes (sem FK)

CREATE TABLE CLASSIFICACAO (
    cod INT PRIMARY KEY,
    nome VARCHAR(50),
    preco FLOAT
);

CREATE TABLE ATOR (
    cod INT PRIMARY KEY,
    dnascimento DATE,
    nacionalidade VARCHAR(50),
    nomereal VARCHAR(50),
    nomeartistico VARCHAR(50)
);

CREATE TABLE CLIENTE (
    num_Cliente INT PRIMARY KEY,
    nome VARCHAR(50),
    endereco VARCHAR(50),
    foneRes VARCHAR(50),
    foneCel VARCHAR(50)
);

-- Tabelas com dependências

CREATE TABLE FILME (
    numFilme INT PRIMARY KEY,
    titulo_original VARCHAR(50),
    titulo_pt VARCHAR(50),
    duracao INT,
    data_lancamento DATE,
    direcao VARCHAR(250),
    categoria VARCHAR(50),
    classificacao INT,
    FOREIGN KEY (classificacao) REFERENCES CLASSIFICACAO(cod)
);

CREATE TABLE MIDIA (
    numFilme INT,
    numero INT,
    tipo VARCHAR(50),
    PRIMARY KEY (numFilme, numero),
    FOREIGN KEY (numFilme) REFERENCES FILME(numFilme)
);

CREATE TABLE ESTRELA (
    num_Filme INT,
    codAtor INT,
    PRIMARY KEY (num_Filme, codAtor),
    FOREIGN KEY (num_Filme) REFERENCES FILME(numFilme),
    FOREIGN KEY (codAtor) REFERENCES ATOR(cod)
);

CREATE TABLE EMPRESTIMO (
    numFilme INT,
    numero INT,
    tipo VARCHAR(50),
    cliente INT,
    dataEmt DATE,
    dateDev DATE,
    valor_pg FLOAT,
    PRIMARY KEY (numFilme, numero),
    FOREIGN KEY (numFilme, numero) REFERENCES MIDIA(numFilme, numero),
    FOREIGN KEY (cliente) REFERENCES CLIENTE(num_Cliente)
);