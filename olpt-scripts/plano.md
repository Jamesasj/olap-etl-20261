# Plano de Geração de Dados — `generate-values.py`

## Observação Importante

O descritivo atual do `generate-values.py` menciona um contexto de **locadora de veículos** (aluguéis, devoluções, manutenção, etc.), porém o DDL define um modelo de **locadora de filmes/vídeos** (FILME, ATOR, MIDIA, EMPRESTIMO, ESTRELA, CLASSIFICACAO, CLIENTE). O plano abaixo segue o **DDL real**.

---

## 1. Estrutura do Banco (Resumo do DDL)

### Tabelas Independentes (sem FK)
| Tabela | PK | Colunas |
|---|---|---|
| CLASSIFICACAO | `cod` | nome, preco |
| ATOR | `cod` | dnascimento, nacionalidade, nomereal, nomeartistico |
| CLIENTE | `num_Cliente` | nome, endereco, foneRes, foneCel |

### Tabelas Dependentes (com FK)
| Tabela | PK | FKs |
|---|---|---|
| FILME | `numFilme` | classificacao → CLASSIFICACAO(cod) |
| MIDIA | `(numFilme, numero)` | numFilme → FILME(numFilme) |
| ESTRELA | `(num_Filme, codAtor)` | num_Filme → FILME(numFilme), codAtor → ATOR(cod) |
| EMPRESTIMO | `(numFilme, numero)` | (numFilme, numero) → MIDIA(numFilme, numero), cliente → CLIENTE(num_Cliente) |

---

## 2. Ordem de Inserção (respeitando integridade referencial)

1. **CLASSIFICACAO**
2. **ATOR**
3. **CLIENTE**
4. **FILME** (depende de CLASSIFICACAO)
5. **MIDIA** (depende de FILME)
6. **ESTRELA** (depende de FILME e ATOR)
7. **EMPRESTIMO** (depende de MIDIA e CLIENTE)

---

## 3. Volumes de Dados Propostos

| Tabela | Quantidade | Justificativa |
|---|---|---|
| CLASSIFICACAO | 5 | Classificações indicativas (Livre, 10, 12, 16, 18) |
| ATOR | 50 | Elenco diversificado |
| CLIENTE | 100 | Base de clientes da locadora |
| FILME | 80 | Catálogo de filmes |
| MIDIA | ~160 | ~2 mídias por filme (DVD, Blu-ray, VHS) |
| ESTRELA | ~200 | ~2-4 atores por filme |
| EMPRESTIMO | 300 | Histórico de empréstimos |

---

## 4. Estratégia de Geração por Tabela

### 4.1 CLASSIFICACAO
- Valores fixos/reais de classificação indicativa brasileira
- Exemplos: (1, 'Livre', 5.00), (2, '10 anos', 6.00), (3, '12 anos', 7.00), (4, '16 anos', 8.50), (5, '18 anos', 10.00)
- Preços crescentes por restrição

### 4.2 ATOR
- `cod`: sequencial 1..50
- `dnascimento`: datas entre 1950 e 2000 (distribuição uniforme)
- `nacionalidade`: pool de nacionalidades (Brasileiro, Americano, Britânico, Francês, etc.)
- `nomereal` e `nomeartistico`: gerados com listas de nomes/sobrenomes realistas

### 4.3 CLIENTE
- `num_Cliente`: sequencial 1..100
- `nome`: combinação de nomes e sobrenomes brasileiros
- `endereco`: ruas fictícias com número
- `foneRes`: formato (XX) XXXX-XXXX
- `foneCel`: formato (XX) 9XXXX-XXXX

### 4.4 FILME
- `numFilme`: sequencial 1..80
- `titulo_original`: títulos gerados ou de uma lista de títulos fictícios em inglês
- `titulo_pt`: versão traduzida/adaptada
- `duracao`: entre 80 e 180 minutos (distribuição normal, média ~120)
- `data_lancamento`: entre 1990 e 2025
- `direcao`: nomes de diretores fictícios
- `categoria`: pool de gêneros (Ação, Comédia, Drama, Terror, Ficção Científica, Romance, Animação, Suspense, Documentário)
- `classificacao`: FK aleatória para CLASSIFICACAO existente

### 4.5 MIDIA
- Para cada filme, gerar 1 a 3 mídias
- `numero`: sequencial por filme (1, 2, 3...)
- `tipo`: escolha entre 'DVD', 'Blu-ray', 'VHS'
- Garantir que cada filme tenha pelo menos 1 mídia

### 4.6 ESTRELA
- Para cada filme, associar 2 a 5 atores aleatórios (sem repetição por filme)
- Verificar unicidade da PK composta (num_Filme, codAtor)

### 4.7 EMPRESTIMO
- `numFilme` e `numero`: FK composta referenciando MIDIA existente
- `tipo`: mesmo tipo da mídia referenciada (consistência)
- `cliente`: FK aleatória para CLIENTE existente
- `dataEmt`: entre 2023 e 2026 (concentração em finais de semana e férias)
- `dateDev`: dataEmt + 1 a 14 dias (alguns nulos para empréstimos não devolvidos)
- `valor_pg`: baseado no preço da classificação do filme, com possíveis acréscimos por atraso

---

## 5. Tecnologias e Bibliotecas

- **Python 3** (sem dependências externas — apenas `random`, `datetime`, `string`)
- Saída: arquivo `.sql` com instruções `INSERT INTO` prontas para execução
- Alternativa: saída direta via `print()` para redirecionamento
- **Nota:** As tabelas já estão criadas no banco. O script gera apenas os `INSERT INTO`, sem executar o `ddl.sql`.

---

## 6. Estrutura do Script

```
generate-values.py
│
├── Constantes e configurações (volumes, pools de dados)
├── Funções auxiliares
│   ├── gerar_nome()
│   ├── gerar_telefone()
│   ├── gerar_endereco()
│   ├── gerar_data(inicio, fim)
│   └── escolher_aleatorio(lista)
├── Funções de geração por tabela
│   ├── gerar_classificacoes()
│   ├── gerar_atores()
│   ├── gerar_clientes()
│   ├── gerar_filmes(classificacoes)
│   ├── gerar_midias(filmes)
│   ├── gerar_estrelas(filmes, atores)
│   └── gerar_emprestimos(midias, clientes, filmes_classificacoes)
└── main()
    ├── Gera dados respeitando ordem de dependência
    └── Escreve arquivo SQL de saída
```

---

## 7. Formato de Saída

Cada execução do script gera **3 arquivos** com número sequencial para controle: `inserts_001a.sql`, `inserts_001b.sql`, `inserts_001c.sql`; na próxima execução `inserts_002a.sql`, `inserts_002b.sql`, `inserts_002c.sql`, etc. O script identifica automaticamente o próximo número disponível no diretório de saída.

**Garantia de não duplicidade:** os 3 arquivos de uma mesma execução contêm dados complementares (sem sobreposição de PKs entre eles). Cada arquivo recebe uma faixa distinta de IDs/registros, permitindo que todos sejam executados no banco sem conflitos.

```sql
-- CLASSIFICACAO
INSERT INTO CLASSIFICACAO (cod, nome, preco) VALUES (1, 'Livre', 5.00);
INSERT INTO CLASSIFICACAO (cod, nome, preco) VALUES (2, '10 anos', 6.00);
...

-- ATOR
INSERT INTO ATOR (cod, dnascimento, nacionalidade, nomereal, nomeartistico) VALUES (...);
...

-- (demais tabelas na ordem de dependência)
```

---

## 8. Validações Garantidas

- [x] Integridade referencial: todas as FKs apontam para PKs existentes
- [x] Unicidade de PKs e PKs compostas
- [x] Tipos de dados compatíveis com o DDL (VARCHAR respeitando tamanho máximo)
- [x] Datas válidas e consistentes (dateDev >= dataEmt)
- [x] Valores numéricos dentro de faixas realistas

---

## Próximo Passo

Aguardando validação deste plano para iniciar a implementação do script.
