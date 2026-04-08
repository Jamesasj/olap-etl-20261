CREATE TABLE IF NOT EXISTS stg_emprestimos (
    id SERIAL PRIMARY KEY,
    cliente_id INT NOT NULL,
    filme_id INT NOT NULL,
    midia_id INT NOT NULL,
    data_aluguel DATE NOT NULL,
    data_devolucao DATE
);