# Databricks notebook source
# MAGIC %md
# MAGIC # Dívida Ativa da União: análise da base SIDA (2º trimestre de 2026)
# MAGIC
# MAGIC **Autora:** Danielli Arçari
# MAGIC
# MAGIC **Objetivo:** entender a composição, a concentração e a recuperabilidade do estoque da dívida ativa da União, a partir da base pública completa da Procuradoria-Geral da Fazenda Nacional (PGFN).
# MAGIC
# MAGIC **Fonte:** Dados Abertos da PGFN (dadosabertos.pgfn.gov.br), arquivo Dados_abertos_Nao_Previdenciario.zip do 2º trimestre de 2026.
# MAGIC
# MAGIC **Recorte:** base SIDA (Dívida Ativa Geral). Ficam de fora a dívida previdenciária e a do FGTS, publicadas em arquivos separados. Os resultados não representam a dívida ativa da União como um todo.
# MAGIC
# MAGIC **Data de referência:** inscrições até 10/07/2026 (data mais recente presente na base).
# MAGIC
# MAGIC **Arquitetura:** o projeto segue o modelo de camadas do Databricks.
# MAGIC - **Bronze:** dados como vieram da fonte, sem alterações
# MAGIC - **Silver:** dados limpos, tipados e sem duplicação
# MAGIC - **Gold:** tabelas agregadas, prontas para análise e gráficos

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Ingestão
# MAGIC
# MAGIC O download é feito direto do servidor da PGFN para um Volume do Databricks, sem passar por máquina local.
# MAGIC
# MAGIC O arquivo compactado tem 1,3 GB e contém 6 arquivos CSV que somam cerca de 9 GB. A divisão entre os arquivos segue a unidade da PGFN responsável pela cobrança, e não o estado do devedor: um devedor de SP pode estar no arquivo de outra região.
# MAGIC
# MAGIC A inspeção das primeiras linhas mostra o formato da fonte: separador ponto e vírgula, codificação latin-1, datas no formato dd/mm/aaaa e valores com ponto decimal, sem separador de milhar.

# COMMAND ----------

import urllib.request, zipfile, os, shutil

pasta_bruto = "/Volumes/workspace/default/dados_divida/bruto/"
os.makedirs(pasta_bruto, exist_ok=True)

url = "https://dadosabertos.pgfn.gov.br/2026_trimestre_02/Dados_abertos_Nao_Previdenciario.zip"
arquivo_zip = pasta_bruto + "SIDA_2026T2.zip"

# Baixa o zip
if not os.path.exists(arquivo_zip):
    print("Baixando... pode levar alguns minutos")
    with urllib.request.urlopen(url, timeout=120) as r, open(arquivo_zip, "wb") as f:
        shutil.copyfileobj(r, f, length=16 * 1024 * 1024)
print(f"Zip salvo: {os.path.getsize(arquivo_zip) / 1e6:,.0f} MB\n")

# Lista o que tem dentro do zip, sem descompactar
with zipfile.ZipFile(arquivo_zip) as z:
    for info in z.infolist():
        print(f"{info.filename:<55} {info.file_size / 1e6:>8,.0f} MB")

    # Mostra as primeiras linhas do primeiro CSV
    primeiro = [i for i in z.infolist() if i.filename.lower().endswith(".csv")][0]
    print(f"\nPrimeiras linhas de {primeiro.filename}:")
    with z.open(primeiro) as f:
        for _ in range(4):
            print(f.readline().decode("latin-1").rstrip())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Camada bronze
# MAGIC
# MAGIC Os 6 CSVs são carregados um de cada vez em uma tabela Delta e apagados logo em seguida. Essa estratégia mantém o uso de espaço baixo, respeitando o limite de armazenamento da edição gratuita do Databricks.
# MAGIC
# MAGIC Nenhum dado é alterado nesta etapa. Todas as colunas são lidas como texto, e são acrescentadas duas colunas de controle: o arquivo de origem e o trimestre de referência.
# MAGIC
# MAGIC **Resultado:** 45.553.971 linhas carregadas.

# COMMAND ----------

import zipfile, os
from pyspark.sql import functions as F

pasta_bruto = "/Volumes/workspace/default/dados_divida/bruto/"
arquivo_zip = pasta_bruto + "SIDA_2026T2.zip"
tabela_bronze = "workspace.default.bronze_sida"

with zipfile.ZipFile(arquivo_zip) as z:
    csvs = sorted(i.filename for i in z.infolist() if i.filename.lower().endswith(".csv"))

    for n, nome in enumerate(csvs):
        print(f"Processando {nome}...")

        # 1. Descompacta só este CSV
        z.extract(nome, pasta_bruto)
        caminho_csv = pasta_bruto + nome

        # 2. Lê o CSV (separador ; e acentuação latin-1), tudo como texto
        df = (spark.read
              .option("header", True)
              .option("sep", ";")
              .option("encoding", "ISO-8859-1")
              .csv(caminho_csv)
              .withColumn("ARQUIVO_ORIGEM", F.lit(nome))
              .withColumn("TRIMESTRE_REF", F.lit("2026-T2")))

        # 3. Grava na tabela Delta (o primeiro arquivo cria, os outros acrescentam)
        modo = "overwrite" if n == 0 else "append"
        df.write.format("delta").mode(modo).saveAsTable(tabela_bronze)

        # 4. Apaga o CSV para liberar espaço
        os.remove(caminho_csv)
        print("  gravado e CSV apagado")

# Conferência: quantas linhas vieram de cada arquivo
display(spark.sql(f"""
    SELECT ARQUIVO_ORIGEM, COUNT(*) AS linhas
    FROM {tabela_bronze}
    GROUP BY ARQUIVO_ORIGEM
    ORDER BY ARQUIVO_ORIGEM
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC Com os dados brutos preservados na tabela bronze, o arquivo compactado é removido para liberar espaço.

# COMMAND ----------

import os
os.remove("/Volumes/workspace/default/dados_divida/bruto/SIDA_2026T2.zip")
print("Zip apagado, 1,3 GB liberados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Diagnóstico de qualidade
# MAGIC
# MAGIC Cada inscrição em dívida ativa aparece na base uma vez para o devedor principal e uma vez para cada corresponsável ou devedor solidário, e todas essas linhas carregam o valor integral da dívida.
# MAGIC
# MAGIC | Tipo de devedor | Linhas | Inscrições distintas | Valor (R$ bilhões) |
# MAGIC |---|---|---|---|
# MAGIC | Principal | 30.845.336 | 30.845.336 | 2.961,82 |
# MAGIC | Corresponsável | 14.656.817 | 12.728.588 | 3.538,12 |
# MAGIC | Solidário | 51.818 | 22.706 | 334,56 |
# MAGIC
# MAGIC Somar todas as linhas resultaria em cerca de R$ 6,83 trilhões, mais que o dobro do estoque real. **Decisão:** todas as análises de valor usam apenas o devedor principal, o que garante uma linha por inscrição.
# MAGIC
# MAGIC Nenhum dos 45,5 milhões de valores apresentou erro de conversão para número.
# MAGIC
# MAGIC **Sobre a versão anterior deste projeto:** a primeira versão usava uma base pré-processada fora do Databricks que tinha dois erros. Os valores estavam multiplicados por 100, porque o ponto decimal foi removido na conversão, e as linhas de corresponsáveis foram somadas junto com as do devedor principal. Esta versão refaz todo o processo a partir da fonte original.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     TIPO_DEVEDOR,
# MAGIC     COUNT(*) AS linhas,
# MAGIC     COUNT(DISTINCT NUMERO_INSCRICAO) AS inscricoes_distintas,
# MAGIC     ROUND(SUM(TRY_CAST(VALOR_CONSOLIDADO AS DECIMAL(18,2))) / 1e9, 2) AS valor_bilhoes,
# MAGIC     SUM(CASE WHEN TRY_CAST(VALOR_CONSOLIDADO AS DECIMAL(18,2)) IS NULL THEN 1 ELSE 0 END) AS valores_invalidos
# MAGIC FROM workspace.default.bronze_sida
# MAGIC GROUP BY TIPO_DEVEDOR
# MAGIC ORDER BY linhas DESC

# COMMAND ----------

# MAGIC %md
# MAGIC **Leitura do resultado:** as linhas de devedor principal (30.845.336) coincidem com as inscrições distintas, o que confirma que cada inscrição tem exatamente um devedor principal. Os corresponsáveis somam R$ 3,54 trilhões, mais que os principais, porque uma mesma dívida pode ter vários corresponsáveis e cada linha repete o valor integral. A coluna de valores inválidos zerada em todos os grupos confirma que a conversão para número não perdeu nenhum registro.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Camada silver
# MAGIC
# MAGIC Duas tabelas são criadas a partir da bronze:
# MAGIC
# MAGIC - **silver_sida:** todas as linhas, com os tipos corrigidos (valor como número decimal, data como data, indicador de ajuizamento como verdadeiro ou falso).
# MAGIC - **silver_inscricoes:** uma linha por inscrição, apenas com o devedor principal, acrescida da quantidade de corresponsáveis, da quantidade de devedores solidários e da idade da dívida em anos.
# MAGIC
# MAGIC A idade é calculada até a data da inscrição mais recente da base, e não até uma data fixa. Assim, o cálculo se ajusta sozinho se o projeto for atualizado com um trimestre novo.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.default.silver_sida AS
# MAGIC SELECT
# MAGIC     CPF_CNPJ,
# MAGIC     TIPO_PESSOA,
# MAGIC     TIPO_DEVEDOR,
# MAGIC     NOME_DEVEDOR,
# MAGIC     NULLIF(TRIM(UF_DEVEDOR), '') AS UF_DEVEDOR,
# MAGIC     UNIDADE_RESPONSAVEL,
# MAGIC     NUMERO_INSCRICAO,
# MAGIC     TIPO_SITUACAO_INSCRICAO,
# MAGIC     SITUACAO_INSCRICAO,
# MAGIC     RECEITA_PRINCIPAL,
# MAGIC     CAST(TRY_TO_TIMESTAMP(DATA_INSCRICAO, 'dd/MM/yyyy') AS DATE) AS DATA_INSCRICAO,
# MAGIC     INDICADOR_AJUIZADO = 'SIM' AS AJUIZADO,
# MAGIC     CAST(VALOR_CONSOLIDADO AS DECIMAL(18,2)) AS VALOR_CONSOLIDADO,
# MAGIC     TRIMESTRE_REF
# MAGIC FROM workspace.default.bronze_sida

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.default.silver_inscricoes AS
# MAGIC WITH outros_devedores AS (
# MAGIC     SELECT
# MAGIC         NUMERO_INSCRICAO,
# MAGIC         SUM(CASE WHEN TIPO_DEVEDOR = 'CORRESPONSAVEL' THEN 1 ELSE 0 END) AS QTD_CORRESPONSAVEIS,
# MAGIC         SUM(CASE WHEN TIPO_DEVEDOR = 'SOLIDARIO' THEN 1 ELSE 0 END) AS QTD_SOLIDARIOS
# MAGIC     FROM workspace.default.silver_sida
# MAGIC     WHERE TIPO_DEVEDOR <> 'PRINCIPAL'
# MAGIC     GROUP BY NUMERO_INSCRICAO
# MAGIC )
# MAGIC SELECT
# MAGIC     p.*,
# MAGIC     COALESCE(o.QTD_CORRESPONSAVEIS, 0) AS QTD_CORRESPONSAVEIS,
# MAGIC     COALESCE(o.QTD_SOLIDARIOS, 0) AS QTD_SOLIDARIOS,
# MAGIC     ROUND(DATEDIFF((SELECT MAX(DATA_INSCRICAO) FROM workspace.default.silver_sida), p.DATA_INSCRICAO) / 365.25, 1) AS IDADE_ANOS
# MAGIC FROM workspace.default.silver_sida p
# MAGIC LEFT JOIN outros_devedores o USING (NUMERO_INSCRICAO)
# MAGIC WHERE p.TIPO_DEVEDOR = 'PRINCIPAL'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS inscricoes,
# MAGIC     ROUND(SUM(VALOR_CONSOLIDADO) / 1e9, 2) AS valor_bilhoes,
# MAGIC     SUM(CASE WHEN DATA_INSCRICAO IS NULL THEN 1 ELSE 0 END) AS datas_invalidas,
# MAGIC     MIN(DATA_INSCRICAO) AS data_mais_antiga,
# MAGIC     MAX(DATA_INSCRICAO) AS data_mais_recente,
# MAGIC     SUM(CASE WHEN UF_DEVEDOR IS NULL THEN 1 ELSE 0 END) AS sem_uf,
# MAGIC     SUM(CASE WHEN QTD_CORRESPONSAVEIS > 0 THEN 1 ELSE 0 END) AS inscricoes_com_corresponsavel
# MAGIC FROM workspace.default.silver_inscricoes

# COMMAND ----------

# MAGIC %md
# MAGIC **Conferência:** 30.845.336 inscrições somando R$ 2.961,82 bilhões, nenhuma data inválida e nenhuma inscrição sem UF. As inscrições vão de 15/10/1973 a 10/07/2026. 41,3% das inscrições (12.728.588) têm pelo menos um corresponsável.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Camada gold
# MAGIC
# MAGIC Três tabelas agregadas alimentam as análises:
# MAGIC
# MAGIC - **gold_situacao:** quantidade, valor total e valor mediano por situação da inscrição e ajuizamento.
# MAGIC - **gold_uf_pessoa:** inscrições, devedores e valor por UF e tipo de pessoa.
# MAGIC - **gold_idade:** inscrições e valor por faixa de idade da dívida.
# MAGIC
# MAGIC Os valores são guardados em reais, sem arredondamento, que só é aplicado na apresentação. A mediana complementa a média porque mostra o valor típico de uma inscrição sem a distorção causada pelas dívidas bilionárias.
# MAGIC
# MAGIC **Limitação:** a PGFN mascara parte do CPF das pessoas físicas. A contagem de devedores pessoa física combina o CPF mascarado com o nome, o que reduz a chance de duas pessoas serem contadas como uma, mas não a elimina. Para pessoa jurídica, a contagem é exata.

# COMMAND ----------

# MAGIC %md
# MAGIC Tabela por situação:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.default.gold_situacao AS
# MAGIC SELECT
# MAGIC     TIPO_SITUACAO_INSCRICAO,
# MAGIC     SITUACAO_INSCRICAO,
# MAGIC     AJUIZADO,
# MAGIC     COUNT(*) AS qtd_inscricoes,
# MAGIC     SUM(VALOR_CONSOLIDADO) AS valor_total,
# MAGIC     PERCENTILE_APPROX(VALOR_CONSOLIDADO, 0.5) AS valor_mediano
# MAGIC FROM workspace.default.silver_inscricoes
# MAGIC GROUP BY TIPO_SITUACAO_INSCRICAO, SITUACAO_INSCRICAO, AJUIZADO

# COMMAND ----------

# MAGIC %md
# MAGIC Tabela por UF e tipo de pessoa:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.default.gold_uf_pessoa AS
# MAGIC SELECT
# MAGIC     CASE WHEN UF_DEVEDOR = 'Si' THEN 'Sem informação' ELSE UF_DEVEDOR END AS UF_DEVEDOR,
# MAGIC     TIPO_PESSOA,
# MAGIC     COUNT(*) AS qtd_inscricoes,
# MAGIC     COUNT(DISTINCT CPF_CNPJ, NOME_DEVEDOR) AS qtd_devedores,
# MAGIC     SUM(VALOR_CONSOLIDADO) AS valor_total
# MAGIC FROM workspace.default.silver_inscricoes
# MAGIC GROUP BY 1, 2

# COMMAND ----------

# MAGIC %md
# MAGIC Tabela por idade da dívida:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.default.gold_idade AS
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN IDADE_ANOS < 1  THEN '1. Menos de 1 ano'
# MAGIC         WHEN IDADE_ANOS < 5  THEN '2. 1 a 5 anos'
# MAGIC         WHEN IDADE_ANOS < 10 THEN '3. 5 a 10 anos'
# MAGIC         WHEN IDADE_ANOS < 15 THEN '4. 10 a 15 anos'
# MAGIC         WHEN IDADE_ANOS < 20 THEN '5. 15 a 20 anos'
# MAGIC         ELSE '6. Mais de 20 anos'
# MAGIC     END AS faixa_idade,
# MAGIC     TIPO_SITUACAO_INSCRICAO,
# MAGIC     AJUIZADO,
# MAGIC     COUNT(*) AS qtd_inscricoes,
# MAGIC     SUM(VALOR_CONSOLIDADO) AS valor_total
# MAGIC FROM workspace.default.silver_inscricoes
# MAGIC GROUP BY 1, 2, 3

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Análises
# MAGIC
# MAGIC Cada análise parte de uma pergunta de negócio, do ponto de vista de quem gerencia a recuperação da dívida ativa. Todos os valores consideram uma linha por inscrição (apenas o devedor principal).

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.1 Composição do estoque por situação
# MAGIC
# MAGIC **Pergunta de negócio:** do estoque total, quanto está efetivamente disponível para cobrança e quanto já está negociado, garantido ou suspenso por decisão judicial?
# MAGIC
# MAGIC **Por que importa:** cada situação exige uma estratégia diferente. Dívida em cobrança pede ação de recuperação; dívida negociada pede monitoramento de pagamento; dívida garantida depende do desfecho do processo; dívida suspensa depende de decisão judicial.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     TIPO_SITUACAO_INSCRICAO,
# MAGIC     SUM(qtd_inscricoes) AS inscricoes,
# MAGIC     ROUND(SUM(valor_total) / 1e9, 2) AS valor_bilhoes,
# MAGIC     ROUND(100 * SUM(valor_total) / SUM(SUM(valor_total)) OVER (), 2) AS perc_valor,
# MAGIC     TRANSLATE(FORMAT_NUMBER(SUM(valor_total) / 1e9, 2), ',.', '.,') AS valor_br
# MAGIC FROM workspace.default.gold_situacao
# MAGIC GROUP BY TIPO_SITUACAO_INSCRICAO
# MAGIC ORDER BY valor_bilhoes DESC

# COMMAND ----------

# MAGIC %md
# MAGIC **Resultado:**
# MAGIC
# MAGIC | Situação | Inscrições | Valor (R$ bi) | % do valor |
# MAGIC |---|---|---|---|
# MAGIC | Em cobrança | 25.068.501 | 1.860,95 | 62,83% |
# MAGIC | Benefício fiscal | 5.673.514 | 603,63 | 20,38% |
# MAGIC | Garantia | 71.394 | 411,19 | 13,88% |
# MAGIC | Suspenso por decisão judicial | 31.889 | 86,04 | 2,91% |
# MAGIC | Em negociação | 38 | 0,01 | 0,00% |
# MAGIC
# MAGIC **Conclusões:**
# MAGIC
# MAGIC - **62,8% do estoque (R$ 1,86 trilhão) está em cobrança**, sem negociação, garantia ou suspensão. É a parte que depende de ação ativa de recuperação. Estar em cobrança, porém, não significa ser recuperável: a análise de idade (6.5) vai mostrar quanto dessa fatia é dívida antiga.
# MAGIC - **20,4% (R$ 603,6 bilhões) está em benefício fiscal**, ou seja, parcelado ou transacionado. Essa receita depende de o devedor continuar pagando: a própria base registra situações de parcelamento rescindido que voltaram à cobrança.
# MAGIC - **A garantia concentra valor em poucas inscrições.** São apenas 71.394 inscrições, 0,23% do total, mas que somam 13,9% do estoque. Em média, cada inscrição garantida vale R$ 5,8 milhões. Como há um bem, depósito ou seguro vinculado, essa é potencialmente a fatia de recuperação mais segura, condicionada ao fim do processo judicial.
# MAGIC - **Apenas 2,9% está suspenso por decisão judicial**, o único grupo efetivamente travado no momento.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 Ajuizamento e valor das dívidas
# MAGIC
# MAGIC **Pergunta de negócio:** a cobrança judicial está direcionada às dívidas de maior valor?
# MAGIC
# MAGIC **Por que importa:** a execução fiscal tem custo alto e duração longa para a Fazenda. Por isso, a Portaria MF nº 75/2012 (art. 1º, II) determina o não ajuizamento de execuções fiscais de débitos de valor consolidado igual ou inferior a R$ 20 mil, admitindo exceções quando houver elemento objetivo que indique elevado potencial de recuperação (§ 6º). Se as dívidas ajuizadas tiverem valor típico muito maior que as não ajuizadas, a estratégia está sendo aplicada na prática.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     TIPO_SITUACAO_INSCRICAO,
# MAGIC     CASE WHEN AJUIZADO THEN 'Ajuizada' ELSE 'Não ajuizada' END AS ajuizamento,
# MAGIC     COUNT(*) AS inscricoes,
# MAGIC     ROUND(SUM(VALOR_CONSOLIDADO) / 1e9, 2) AS valor_bilhoes,
# MAGIC     ROUND(100 * SUM(VALOR_CONSOLIDADO) / SUM(SUM(VALOR_CONSOLIDADO)) OVER (), 2) AS perc_valor,
# MAGIC     ROUND(PERCENTILE_APPROX(VALOR_CONSOLIDADO, 0.5), 2) AS valor_mediano_reais,
# MAGIC     TRANSLATE(FORMAT_NUMBER(SUM(VALOR_CONSOLIDADO) / 1e9, 2), ',.', '.,') AS valor_br
# MAGIC FROM workspace.default.silver_inscricoes
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY valor_bilhoes DESC

# COMMAND ----------

# MAGIC %md
# MAGIC **Resultado (principais grupos):**
# MAGIC
# MAGIC | Situação | Ajuizamento | Inscrições | Valor (R$ bi) | % do valor | Mediana aproximada (R$) |
# MAGIC |---|---|---|---|---|---|
# MAGIC | Em cobrança | Ajuizada | 4.787.103 | 1.442,71 | 48,71% | 19.554,36 |
# MAGIC | Em cobrança | Não ajuizada | 20.281.398 | 418,24 | 14,12% | 2.693,16 |
# MAGIC | Benefício fiscal | Ajuizada | 1.216.448 | 414,83 | 14,01% | 19.690,42 |
# MAGIC | Garantia | Ajuizada | 65.994 | 407,24 | 13,75% | 94.218,94 |
# MAGIC | Benefício fiscal | Não ajuizada | 4.457.066 | 188,81 | 6,37% | 4.234,52 |
# MAGIC
# MAGIC **Conclusões:**
# MAGIC
# MAGIC - **A estratégia de ajuizamento seletivo aparece com clareza nos dados.** As inscrições ajuizadas são cerca de 20% do total (6,08 milhões), mas concentram 78% do valor (R$ 2,31 trilhões).
# MAGIC - **Dentro da dívida em cobrança, a inscrição ajuizada típica vale 7 vezes mais que a não ajuizada**: mediana de R$ 19.554 contra R$ 2.693.
# MAGIC - **Quase metade de todo o estoque (48,7%, R$ 1,44 trilhão) está em execução fiscal sem garantia e sem negociação.** É o maior bloco da carteira, e sua recuperação depende do andamento de processos judiciais em que a Fazenda ainda não tem um bem vinculado à dívida.
# MAGIC - **Na outra ponta, há uma massa de 20,3 milhões de inscrições em cobrança não ajuizadas**, 66% de todas as inscrições, com mediana de R$ 2.693. Somam R$ 418 bilhões. Pelo volume e pelo valor baixo, é uma carteira que só pode ser trabalhada com instrumentos de cobrança em massa, como protesto e transação por adesão, e não caso a caso.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 6.2.1 Ajuizamento e o limite da Portaria MF nº 75/2012
# MAGIC
# MAGIC **Pergunta de negócio:** se a mediana das inscrições em cobrança ajuizadas está abaixo de R$ 20 mil, o limite da portaria está sendo descumprido, ou há explicações dentro da própria regra?
# MAGIC
# MAGIC **Hipóteses testadas:** (1) parte das inscrições foi ajuizada antes da portaria; (2) o limite é aplicado sobre a soma dos débitos do devedor, e não sobre cada inscrição isolada.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH ajuizadas AS (
# MAGIC     SELECT
# MAGIC         CASE WHEN DATA_INSCRICAO < DATE'2012-03-29'
# MAGIC              THEN '1. Inscrita antes da portaria'
# MAGIC              ELSE '2. Inscrita depois da portaria'
# MAGIC         END AS periodo,
# MAGIC         VALOR_CONSOLIDADO,
# MAGIC         SUM(VALOR_CONSOLIDADO) OVER (PARTITION BY CPF_CNPJ, NOME_DEVEDOR) AS total_do_devedor
# MAGIC     FROM workspace.default.silver_inscricoes
# MAGIC     WHERE AJUIZADO
# MAGIC )
# MAGIC SELECT
# MAGIC     periodo,
# MAGIC     CASE
# MAGIC         WHEN VALOR_CONSOLIDADO > 20000 THEN 'a. Inscrição acima de R$ 20 mil'
# MAGIC         WHEN total_do_devedor > 20000  THEN 'b. Inscrição até R$ 20 mil, devedor acima'
# MAGIC         ELSE 'c. Inscrição e devedor até R$ 20 mil'
# MAGIC     END AS enquadramento,
# MAGIC     COUNT(*) AS inscricoes,
# MAGIC     ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY periodo), 1) AS perc_no_periodo
# MAGIC FROM ajuizadas
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 1, 2

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH ajuizadas AS (
# MAGIC     SELECT
# MAGIC         VALOR_CONSOLIDADO,
# MAGIC         SUM(VALOR_CONSOLIDADO) OVER (PARTITION BY CPF_CNPJ, NOME_DEVEDOR) AS total_do_devedor
# MAGIC     FROM workspace.default.silver_inscricoes
# MAGIC     WHERE AJUIZADO
# MAGIC )
# MAGIC SELECT '1. Inscrições ajuizadas' AS etapa, COUNT(*) AS inscricoes
# MAGIC FROM ajuizadas
# MAGIC UNION ALL
# MAGIC SELECT '2. Com valor individual até R$ 20 mil', COUNT(*)
# MAGIC FROM ajuizadas
# MAGIC WHERE VALOR_CONSOLIDADO <= 20000
# MAGIC UNION ALL
# MAGIC SELECT '3. E com devedor até R$ 20 mil no total', COUNT(*)
# MAGIC FROM ajuizadas
# MAGIC WHERE VALOR_CONSOLIDADO <= 20000 AND total_do_devedor <= 20000

# COMMAND ----------

# MAGIC %md
# MAGIC **Resultado:**
# MAGIC
# MAGIC | Período de inscrição | Enquadramento | Inscrições ajuizadas | % no período |
# MAGIC |---|---|---|---|
# MAGIC | Antes da portaria | Inscrição acima de R$ 20 mil | 412.867 | 57,3% |
# MAGIC | Antes da portaria | Inscrição até R$ 20 mil, devedor acima | 269.479 | 37,4% |
# MAGIC | Antes da portaria | Inscrição e devedor até R$ 20 mil | 37.836 | 5,3% |
# MAGIC | Depois da portaria | Inscrição acima de R$ 20 mil | 2.617.918 | 48,8% |
# MAGIC | Depois da portaria | Inscrição até R$ 20 mil, devedor acima | 2.718.433 | 50,7% |
# MAGIC | Depois da portaria | Inscrição e devedor até R$ 20 mil | 23.831 | 0,4% |
# MAGIC
# MAGIC **Conclusões:**
# MAGIC
# MAGIC - **O funil resume o achado:** das 6.080.364 inscrições ajuizadas, 3.049.579 (50,2%) têm valor individual de até R$ 20 mil. Mas, quando se soma tudo o que cada devedor deve, apenas 61.667 (1,0%) continuam abaixo do limite.
# MAGIC - **Os dados são compatíveis com a aplicação do limite sobre a soma dos débitos do devedor, e não sobre a inscrição isolada.** Entre as inscrições ajuizadas depois da portaria, metade (50,7%) tem valor individual de até R$ 20 mil, mas pertence a devedores cuja dívida somada passa desse limite. Isso explica por que a mediana das inscrições ajuizadas em cobrança (R$ 19.554) fica abaixo do piso.
# MAGIC - **Apenas 0,4% das inscrições ajuizadas depois da portaria (23.831) ficam abaixo de R$ 20 mil mesmo somando todas as dívidas do devedor.** Esse resíduo é compatível com as exceções do § 6º e com dívidas que diminuíram após pagamentos parciais.
# MAGIC - **Antes da portaria, esse resíduo era maior (5,3%)**, o que é coerente com a vigência de um limite anterior mais baixo.
# MAGIC - **A hipótese do período explica pouco:** só 11,8% das inscrições ajuizadas (720.182 de 6.080.364) são anteriores à portaria. A explicação principal é a soma por devedor.
# MAGIC
# MAGIC **Limitação:** os valores da base são os atualizados até 2026. Na data do ajuizamento, as dívidas eram menores, então o grupo abaixo do limite estava ainda mais abaixo naquela época.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 Concentração da dívida por devedor
# MAGIC
# MAGIC **Pergunta de negócio:** quantos devedores respondem pela maior parte do estoque?
# MAGIC
# MAGIC **Por que importa:** se poucos devedores concentram a maior parte do valor, uma equipe dedicada a grandes devedores pode ter mais impacto na recuperação do que ações sobre milhões de inscrições pequenas.
# MAGIC
# MAGIC **Nota de método:** empresas são agrupadas pela raiz do CNPJ (os 8 primeiros dígitos), para que matriz e filiais contem como um único devedor. Pessoas físicas são agrupadas por CPF mascarado e nome.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH devedores AS (
# MAGIC     SELECT
# MAGIC         CASE WHEN TIPO_PESSOA = 'Pessoa jurídica'
# MAGIC              THEN LEFT(CPF_CNPJ, 10)
# MAGIC              ELSE CONCAT(CPF_CNPJ, NOME_DEVEDOR)
# MAGIC         END AS id_devedor,
# MAGIC         SUM(VALOR_CONSOLIDADO) AS divida_total
# MAGIC     FROM workspace.default.silver_inscricoes
# MAGIC     GROUP BY 1
# MAGIC )
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN divida_total < 10000     THEN '1. Abaixo de R$ 10 mil'
# MAGIC         WHEN divida_total < 100000    THEN '2. R$ 10 mil a R$ 100 mil'
# MAGIC         WHEN divida_total < 1000000   THEN '3. R$ 100 mil a R$ 1 milhão'
# MAGIC         WHEN divida_total < 15000000  THEN '4. R$ 1 milhão a R$ 15 milhões'
# MAGIC         WHEN divida_total < 100000000 THEN '5. R$ 15 milhões a R$ 100 milhões'
# MAGIC         ELSE '6. Acima de R$ 100 milhões'
# MAGIC     END AS faixa_divida,
# MAGIC     COUNT(*) AS devedores,
# MAGIC     ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS perc_devedores,
# MAGIC     ROUND(SUM(divida_total) / 1e9, 2) AS valor_bilhoes,
# MAGIC     ROUND(100 * SUM(divida_total) / SUM(SUM(divida_total)) OVER (), 2) AS perc_valor,
# MAGIC     CONCAT(TRANSLATE(FORMAT_NUMBER(100 * SUM(divida_total) / SUM(SUM(divida_total)) OVER (), 1), ',.', '.,'), '%') AS perc_br
# MAGIC FROM devedores
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %md
# MAGIC **Resultado:**
# MAGIC
# MAGIC | Faixa de dívida do devedor | Devedores | % dos devedores | Valor (R$ bi) | % do valor |
# MAGIC |---|---|---|---|---|
# MAGIC | Abaixo de R$ 10 mil | 5.536.759 | 67,49% | 20,20 | 0,68% |
# MAGIC | R$ 10 mil a R$ 100 mil | 1.776.718 | 21,66% | 63,08 | 2,13% |
# MAGIC | R$ 100 mil a R$ 1 milhão | 693.566 | 8,45% | 213,19 | 7,20% |
# MAGIC | R$ 1 milhão a R$ 15 milhões | 177.411 | 2,16% | 564,25 | 19,05% |
# MAGIC | R$ 15 milhões a R$ 100 milhões | 16.534 | 0,20% | 581,77 | 19,64% |
# MAGIC | Acima de R$ 100 milhões | 3.270 | 0,04% | 1.519,34 | 51,30% |
# MAGIC
# MAGIC **Conclusões:**
# MAGIC
# MAGIC - **A dívida ativa é extremamente concentrada.** Apenas 3.270 devedores, 0,04% do total de 8,2 milhões, respondem por mais da metade do estoque: R$ 1,52 trilhão.
# MAGIC - **Menos de 20 mil devedores concentram 71% do valor.** Somando as duas faixas acima de R$ 15 milhões, são 19.804 devedores (0,24%) com R$ 2,10 trilhões.
# MAGIC - **Na outra ponta, 89% dos devedores devem menos de R$ 100 mil cada**, e juntos somam apenas 2,8% do estoque.
# MAGIC - **Implicação para a gestão:** a recuperação do estoque depende de um grupo pequeno o bastante para ser acompanhado individualmente por uma equipe especializada. Já os 7,3 milhões de devedores abaixo de R$ 100 mil só podem ser tratados com cobrança automatizada e em massa, em linha com o que a análise 6.2 mostrou para as inscrições não ajuizadas.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.4 Distribuição geográfica
# MAGIC
# MAGIC **Pergunta de negócio:** em quais estados a dívida está concentrada, e ela é principalmente de empresas ou de pessoas físicas?
# MAGIC
# MAGIC **Por que importa:** orienta a alocação de esforço entre as unidades regionais da PGFN e mostra se a estratégia deve mirar empresas ou pessoas em cada estado.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     UF_DEVEDOR,
# MAGIC     ROUND(SUM(CASE WHEN TIPO_PESSOA = 'Pessoa jurídica' THEN valor_total END) / 1e9, 2) AS pj_bilhoes,
# MAGIC     ROUND(SUM(CASE WHEN TIPO_PESSOA = 'Pessoa física' THEN valor_total END) / 1e9, 2) AS pf_bilhoes,
# MAGIC     ROUND(SUM(valor_total) / 1e9, 2) AS total_bilhoes,
# MAGIC     ROUND(100 * SUM(valor_total) / SUM(SUM(valor_total)) OVER (), 2) AS perc_valor,
# MAGIC     ROUND(100 * SUM(CASE WHEN TIPO_PESSOA = 'Pessoa jurídica' THEN valor_total END) / SUM(valor_total), 1) AS perc_pj
# MAGIC FROM workspace.default.gold_uf_pessoa
# MAGIC GROUP BY UF_DEVEDOR
# MAGIC ORDER BY total_bilhoes DESC

# COMMAND ----------

# MAGIC %md
# MAGIC **Verificação das UFs fora do padrão:** a consulta abaixo lista as inscrições cuja UF não corresponde a nenhuma das 27 unidades da federação, para confirmar se há erro de digitação, espaço ou código especial antes de interpretar a distribuição geográfica. Ela consulta a silver, onde o código original "Si" foi preservado; na camada gold, esses registros aparecem como "Sem informação".

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     UF_DEVEDOR,
# MAGIC     LENGTH(UF_DEVEDOR) AS tamanho_do_texto,
# MAGIC     TIPO_PESSOA,
# MAGIC     UNIDADE_RESPONSAVEL,
# MAGIC     COUNT(*) AS inscricoes,
# MAGIC     ROUND(SUM(VALOR_CONSOLIDADO) / 1e6, 2) AS valor_milhoes,
# MAGIC     MAX(NOME_DEVEDOR) AS exemplo_de_nome
# MAGIC FROM workspace.default.silver_inscricoes
# MAGIC WHERE UF_DEVEDOR NOT IN ('AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG',
# MAGIC                          'PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO')
# MAGIC GROUP BY 1, 2, 3, 4
# MAGIC ORDER BY inscricoes DESC

# COMMAND ----------

# MAGIC %md
# MAGIC **Resultado (5 maiores estados por valor):**
# MAGIC
# MAGIC | UF | Empresas (R$ bi) | Pessoas físicas (R$ bi) | Total (R$ bi) | % do valor | % empresas |
# MAGIC |---|---|---|---|---|---|
# MAGIC | SP | 1.157,04 | 50,36 | 1.207,40 | 40,77% | 95,8% |
# MAGIC | RJ | 416,94 | 16,48 | 433,42 | 14,63% | 96,2% |
# MAGIC | MG | 164,52 | 13,06 | 177,58 | 6,00% | 92,6% |
# MAGIC | PR | 127,94 | 10,38 | 138,32 | 4,67% | 92,5% |
# MAGIC | RS | 126,87 | 8,40 | 135,27 | 4,57% | 93,8% |
# MAGIC
# MAGIC **Conclusões:**
# MAGIC
# MAGIC - **São Paulo concentra 40,8% do estoque (R$ 1,21 trilhão)**, e SP e RJ juntos somam 55,4%. Os quatro estados do Sudeste chegam a 63,4%.
# MAGIC - **A dívida é essencialmente empresarial em todo o país.** Em todos os estados, as empresas respondem por mais de 75% do valor. A participação das pessoas físicas é maior no Distrito Federal (22,5%, R$ 13,78 bilhões) e nos estados da região Norte com menor estoque, como Acre (23,4%), Roraima (21,5%) e Amapá (20,9%), pontos que mereceriam investigação própria.
# MAGIC - **Limitação importante:** a UF registrada é a do domicílio fiscal do devedor. Grandes grupos costumam ter sede em SP ou RJ mesmo atuando no país inteiro, então a concentração geográfica reflete em parte onde estão as sedes, e não onde a atividade econômica acontece. Isso se conecta à análise 6.3: poucos grandes devedores, sediados em poucos estados, explicam boa parte da concentração.
# MAGIC - **A cauda é longa.** Os 12 estados com menor estoque, do Maranhão a Roraima, somam juntos cerca de R$ 191,6 bilhões, 6,5% do total, pouco mais que Minas Gerais sozinho (R$ 177,58 bilhões).
# MAGIC - **Registros sem identificação:** 245 inscrições (R$ 43,24 milhões) aparecem com a UF "Si", abreviação de "Sem informação", e sem nome do devedor. Representam 0,00% do estoque e não afetam as conclusões, mas foram mantidas na base e identificadas como "Sem informação".

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH top_ufs AS (
# MAGIC     SELECT UF_DEVEDOR
# MAGIC     FROM workspace.default.gold_uf_pessoa
# MAGIC     WHERE UF_DEVEDOR <> 'Sem informação'
# MAGIC     GROUP BY UF_DEVEDOR
# MAGIC     ORDER BY SUM(valor_total) DESC
# MAGIC     LIMIT 10
# MAGIC )
# MAGIC SELECT
# MAGIC     g.UF_DEVEDOR,
# MAGIC     g.TIPO_PESSOA,
# MAGIC     ROUND(g.valor_total / 1e9, 2) AS valor_bilhoes,
# MAGIC     TRANSLATE(FORMAT_NUMBER(g.valor_total / 1e9, 2), ',.', '.,') AS valor_br,
# MAGIC     SUM(g.valor_total) OVER (PARTITION BY g.UF_DEVEDOR) AS total_uf
# MAGIC FROM workspace.default.gold_uf_pessoa g
# MAGIC JOIN top_ufs t ON g.UF_DEVEDOR = t.UF_DEVEDOR
# MAGIC ORDER BY total_uf DESC, g.TIPO_PESSOA DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.5 Idade do estoque
# MAGIC
# MAGIC **Pergunta de negócio:** a dívida em cobrança está sendo resolvida ou está envelhecendo no estoque?
# MAGIC
# MAGIC **Por que importa:** quanto mais antiga a dívida, menor a chance de recuperação. Empresas encerram atividades, patrimônio se dispersa e, nas execuções paradas, corre a prescrição intercorrente (art. 40 da Lei nº 6.830/1980). Uma carteira envelhecida em cobrança indica valor contábil que dificilmente vira receita.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     faixa_idade,
# MAGIC     SUM(qtd_inscricoes) AS inscricoes,
# MAGIC     ROUND(SUM(valor_total) / 1e9, 2) AS valor_bilhoes,
# MAGIC     ROUND(100 * SUM(valor_total) / SUM(SUM(valor_total)) OVER (), 2) AS perc_valor,
# MAGIC     ROUND(100 * SUM(CASE WHEN TIPO_SITUACAO_INSCRICAO = 'Em cobrança' THEN valor_total END) / SUM(valor_total), 1) AS perc_em_cobranca,
# MAGIC     ROUND(100 * SUM(CASE WHEN TIPO_SITUACAO_INSCRICAO = 'Benefício Fiscal' THEN valor_total END) / SUM(valor_total), 1) AS perc_beneficio,
# MAGIC     ROUND(100 * SUM(CASE WHEN TIPO_SITUACAO_INSCRICAO = 'Garantia' THEN valor_total END) / SUM(valor_total), 1) AS perc_garantia
# MAGIC FROM workspace.default.gold_idade
# MAGIC GROUP BY faixa_idade
# MAGIC ORDER BY faixa_idade

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT faixa_idade, situacao, perc_valor, perc_br
# MAGIC FROM (
# MAGIC     SELECT
# MAGIC         faixa_idade,
# MAGIC         TIPO_SITUACAO_INSCRICAO AS situacao,
# MAGIC         ROUND(100 * SUM(valor_total) / SUM(SUM(valor_total)) OVER (PARTITION BY faixa_idade), 1) AS perc_valor,
# MAGIC         CONCAT(TRANSLATE(FORMAT_NUMBER(
# MAGIC             100 * SUM(valor_total) / SUM(SUM(valor_total)) OVER (PARTITION BY faixa_idade), 1),
# MAGIC             ',.', '.,'), '%') AS perc_br
# MAGIC     FROM workspace.default.gold_idade
# MAGIC     GROUP BY faixa_idade, TIPO_SITUACAO_INSCRICAO
# MAGIC )
# MAGIC WHERE situacao IN ('Em cobrança', 'Benefício Fiscal', 'Garantia')
# MAGIC ORDER BY faixa_idade, situacao

# COMMAND ----------

# MAGIC %md
# MAGIC **Resultado:**
# MAGIC
# MAGIC | Idade da dívida | Inscrições | Valor (R$ bi) | % do valor | % em cobrança | % benefício fiscal | % garantia |
# MAGIC |---|---|---|---|---|---|---|
# MAGIC | Menos de 1 ano | 7.329.063 | 282,11 | 9,52% | 74,5% | 8,4% | 14,8% |
# MAGIC | 1 a 5 anos | 17.890.330 | 886,03 | 29,91% | 56,7% | 26,6% | 13,8% |
# MAGIC | 5 a 10 anos | 3.929.314 | 825,19 | 27,86% | 55,7% | 20,8% | 20,0% |
# MAGIC | 10 a 15 anos | 995.184 | 444,27 | 15,00% | 59,6% | 26,3% | 12,2% |
# MAGIC | 15 a 20 anos | 323.664 | 264,83 | 8,94% | 79,2% | 10,5% | 7,1% |
# MAGIC | Mais de 20 anos | 377.781 | 259,39 | 8,76% | 82,7% | 11,1% | 3,5% |
# MAGIC
# MAGIC **Conclusões:**
# MAGIC
# MAGIC - **Um terço do estoque tem mais de 10 anos.** São R$ 968,5 bilhões (32,7% do valor) em apenas 1,7 milhão de inscrições, 5,5% do total. A dívida antiga é formada por poucas inscrições de valor alto.
# MAGIC - **Depois dos 15 anos, a dívida volta a se concentrar em cobrança sem solução.** Entre 1 e 15 anos, a parcela em cobrança fica estável entre 56% e 60%, porque parte do estoque é negociada ou garantida. Na faixa de 15 a 20 anos, a cobrança sobe para 79,2%, e acima de 20 anos chega a 82,7%. A garantia, que alcança 20,0% do valor na faixa de 5 a 10 anos, cai para 3,5% nas dívidas com mais de 20 anos.
# MAGIC - **Implicação:** a dívida que não foi negociada nem garantida nos primeiros anos tende a permanecer no estoque sem perspectiva de solução, sujeita à prescrição intercorrente. Esse é o bloco com maior probabilidade de ser valor contábil sem retorno financeiro.
# MAGIC - **A dívida recente também chama atenção:** 74,5% do valor inscrito no último ano está em cobrança, sem negociação. É a janela em que a cobrança tem mais chance de sucesso, antes que o devedor perca patrimônio.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.6 Origem da dívida
# MAGIC
# MAGIC **Pergunta de negócio:** quais tributos e receitas geram a maior parte do estoque?
# MAGIC
# MAGIC **Por que importa:** identificar as receitas que mais geram dívida ajuda a direcionar ações de prevenção junto à Receita Federal e a desenhar programas de negociação específicos para cada tipo de débito.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     RECEITA_PRINCIPAL,
# MAGIC         CASE
# MAGIC         WHEN TRIM(REPLACE(REPLACE(REPLACE(RECEITA_PRINCIPAL, 'Receita da dívida ativa - ', ''), 'R D Ativa - ', ''), ' - Receita da dívida ativa', '')) = 'Outras'
# MAGIC         THEN 'Outras (categoria da PGFN)'
# MAGIC         ELSE TRIM(REPLACE(REPLACE(REPLACE(RECEITA_PRINCIPAL, 'Receita da dívida ativa - ', ''), 'R D Ativa - ', ''), ' - Receita da dívida ativa', ''))
# MAGIC     END AS receita,
# MAGIC     COUNT(*) AS inscricoes,
# MAGIC     ROUND(SUM(VALOR_CONSOLIDADO) / 1e9, 2) AS valor_bilhoes,
# MAGIC     ROUND(100 * SUM(VALOR_CONSOLIDADO) / SUM(SUM(VALOR_CONSOLIDADO)) OVER (), 2) AS perc_valor,
# MAGIC     ROUND(PERCENTILE_APPROX(VALOR_CONSOLIDADO, 0.5), 2) AS valor_mediano_reais,
# MAGIC     TRANSLATE(FORMAT_NUMBER(SUM(VALOR_CONSOLIDADO) / 1e9, 2), ',.', '.,') AS valor_br
# MAGIC FROM workspace.default.silver_inscricoes
# MAGIC GROUP BY RECEITA_PRINCIPAL
# MAGIC ORDER BY valor_bilhoes DESC
# MAGIC LIMIT 15

# COMMAND ----------

# MAGIC %md
# MAGIC **Resultado (10 maiores receitas por valor):**
# MAGIC
# MAGIC | Receita | Inscrições | Valor (R$ bi) | % do valor | Mediana (R$) |
# MAGIC |---|---|---|---|---|
# MAGIC | IRPJ | 1.806.581 | 618,83 | 20,89% | 11.991 |
# MAGIC | COFINS | 1.762.586 | 523,53 | 17,68% | 13.268 |
# MAGIC | CSLL | 1.732.608 | 254,99 | 8,61% | 9.105 |
# MAGIC | Simples Nacional | 3.593.295 | 220,90 | 7,46% | 10.591 |
# MAGIC | IRRF | 872.702 | 194,02 | 6,55% | 3.299 |
# MAGIC | IPI | 115.977 | 182,55 | 6,16% | 104.632 |
# MAGIC | Contribuição Empresa/Empregador | 1.678.987 | 164,22 | 5,54% | 6.305 |
# MAGIC | IRPF | 1.942.318 | 121,46 | 4,10% | 8.450 |
# MAGIC | PIS | 1.254.278 | 117,33 | 3,96% | 5.281 |
# MAGIC | Multa Isolada | 1.034.591 | 102,53 | 3,46% | 2.395 |
# MAGIC
# MAGIC **Conclusões:**
# MAGIC
# MAGIC - **Três tributos sobre empresas respondem por quase metade do estoque.** IRPJ, COFINS e CSLL somam R$ 1,40 trilhão, 47,2% do valor. Só os tributos sobre o lucro (IRPJ e CSLL) chegam a 29,5%.
# MAGIC - **O Simples Nacional tem o maior número de inscrições da lista (3,6 milhões)**, mas só 7,5% do valor. É a dívida das micro e pequenas empresas: muitos devedores, valores menores.
# MAGIC - **O IPI é o oposto:** apenas 116 mil inscrições, mas com mediana de R$ 104.632, de longe a maior da lista. É uma dívida de poucos devedores grandes, típica da indústria.
# MAGIC - **Implicação:** a carteira pede estratégias distintas por origem. Programas de negociação em massa fazem sentido para o Simples Nacional; para IRPJ, CSLL e IPI, o caminho é a atuação individualizada sobre os grandes devedores identificados na análise 6.3.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.7 Corresponsáveis
# MAGIC
# MAGIC **Pergunta de negócio:** em quais dívidas a PGFN tem mais de uma pessoa a quem cobrar, e quanto do estoque isso representa?
# MAGIC
# MAGIC **Por que importa:** quando há corresponsáveis, como sócios e administradores na hipótese do art. 135, III, do CTN, a cobrança pode alcançar o patrimônio deles além do patrimônio do devedor principal. Isso amplia as chances de recuperação, principalmente em dívidas de empresas que encerraram atividades.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN VALOR_CONSOLIDADO < 10000     THEN '1. Abaixo de R$ 10 mil'
# MAGIC         WHEN VALOR_CONSOLIDADO < 100000    THEN '2. R$ 10 mil a R$ 100 mil'
# MAGIC         WHEN VALOR_CONSOLIDADO < 1000000   THEN '3. R$ 100 mil a R$ 1 milhão'
# MAGIC         WHEN VALOR_CONSOLIDADO < 15000000  THEN '4. R$ 1 milhão a R$ 15 milhões'
# MAGIC         WHEN VALOR_CONSOLIDADO < 100000000 THEN '5. R$ 15 milhões a R$ 100 milhões'
# MAGIC         ELSE '6. Acima de R$ 100 milhões'
# MAGIC     END AS faixa_inscricao,
# MAGIC     COUNT(*) AS inscricoes,
# MAGIC     ROUND(100 * AVG(CASE WHEN QTD_CORRESPONSAVEIS > 0 THEN 1 ELSE 0 END), 1) AS perc_com_corresponsavel,
# MAGIC     ROUND(100 * SUM(CASE WHEN QTD_CORRESPONSAVEIS > 0 THEN VALOR_CONSOLIDADO ELSE 0 END) / SUM(VALOR_CONSOLIDADO), 1) AS perc_valor_com_corresponsavel,
# MAGIC     ROUND(AVG(CASE WHEN QTD_CORRESPONSAVEIS > 0 THEN QTD_CORRESPONSAVEIS END), 1) AS media_corresponsaveis_quando_ha,
# MAGIC     TRANSLATE(FORMAT_NUMBER(AVG(CASE WHEN QTD_CORRESPONSAVEIS > 0 THEN QTD_CORRESPONSAVEIS END), 1), ',.', '.,') AS media_br
# MAGIC FROM workspace.default.silver_inscricoes
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %md
# MAGIC **Resultado:**
# MAGIC
# MAGIC | Faixa de valor da inscrição | Inscrições | % com corresponsável | % do valor com corresponsável | Média de corresponsáveis (quando há) |
# MAGIC |---|---|---|---|---|
# MAGIC | Abaixo de R$ 10 mil | 21.734.186 | 47,4% | 45,4% | 1,1 |
# MAGIC | R$ 10 mil a R$ 100 mil | 7.030.451 | 26,3% | 25,7% | 1,3 |
# MAGIC | R$ 100 mil a R$ 1 milhão | 1.810.078 | 26,5% | 27,6% | 1,8 |
# MAGIC | R$ 1 milhão a R$ 15 milhões | 251.339 | 34,2% | 35,0% | 2,7 |
# MAGIC | R$ 15 milhões a R$ 100 milhões | 16.938 | 38,5% | 38,8% | 4,2 |
# MAGIC | Acima de R$ 100 milhões | 2.344 | 37,4% | 33,1% | 5,2 |
# MAGIC
# MAGIC **Conclusões:**
# MAGIC
# MAGIC - **Quanto maior a dívida, mais pessoas respondem por ela.** Quando há corresponsáveis, a média sobe de 1,1 pessoa nas inscrições abaixo de R$ 10 mil para 5,2 pessoas nas inscrições acima de R$ 100 milhões. Nas dívidas grandes, a cobrança pode alcançar o patrimônio de vários sócios e administradores além do devedor principal.
# MAGIC - **A partir de R$ 1 milhão, mais de um terço das inscrições tem corresponsável** (entre 34,2% e 38,5%), contra cerca de 26% nas faixas intermediárias.
# MAGIC - **O dado inesperado está nas dívidas pequenas:** 47,4% das inscrições abaixo de R$ 10 mil têm corresponsável, a maior proporção de todas as faixas, quase sempre com uma única pessoa. É uma hipótese a investigar se esse padrão vem de pequenas empresas encerradas irregularmente, em que o sócio passa a responder pela dívida.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.8 Maiores devedores
# MAGIC
# MAGIC **Pergunta de negócio:** os 20 maiores devedores empresariais estão em cobrança ativa, negociados ou garantidos? Quantos estão falidos ou em recuperação judicial?
# MAGIC
# MAGIC **Por que importa:** a análise 6.3 mostrou que poucos devedores concentram a maior parte do estoque. A situação de cada um define se há espaço real para recuperação ou se a dívida depende de processos de falência.
# MAGIC
# MAGIC **Nota de método:** empresas agrupadas pela raiz do CNPJ. O indicador de falência ou recuperação judicial é aproximado, baseado em termos presentes no nome registrado do devedor.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     LEFT(CPF_CNPJ, 10) AS raiz_cnpj,
# MAGIC     MAX(NOME_DEVEDOR) AS nome,
# MAGIC     COUNT(*) AS inscricoes,
# MAGIC     ROUND(SUM(VALOR_CONSOLIDADO) / 1e9, 2) AS divida_bilhoes,
# MAGIC     ROUND(100 * SUM(CASE WHEN TIPO_SITUACAO_INSCRICAO = 'Em cobrança' THEN VALOR_CONSOLIDADO ELSE 0 END) / SUM(VALOR_CONSOLIDADO), 1) AS perc_cobranca,
# MAGIC     ROUND(100 * SUM(CASE WHEN TIPO_SITUACAO_INSCRICAO = 'Benefício Fiscal' THEN VALOR_CONSOLIDADO ELSE 0 END) / SUM(VALOR_CONSOLIDADO), 1) AS perc_beneficio,
# MAGIC     ROUND(100 * SUM(CASE WHEN TIPO_SITUACAO_INSCRICAO = 'Garantia' THEN VALOR_CONSOLIDADO ELSE 0 END) / SUM(VALOR_CONSOLIDADO), 1) AS perc_garantia,
# MAGIC     ROUND(100 * SUM(CASE WHEN TIPO_SITUACAO_INSCRICAO = 'Suspenso por decisão judicial' THEN VALOR_CONSOLIDADO ELSE 0 END) / SUM(VALOR_CONSOLIDADO), 1) AS perc_suspenso,
# MAGIC     MIN(DATA_INSCRICAO) AS inscricao_mais_antiga,
# MAGIC     MAX(CASE WHEN NOME_DEVEDOR LIKE '%FALID%'
# MAGIC               OR NOME_DEVEDOR LIKE '%RECUPERACAO JUDICIAL%'
# MAGIC               OR NOME_DEVEDOR LIKE '%LIQUIDACAO%'
# MAGIC              THEN 'Sim' ELSE 'Não' END) AS indicio_falencia_rj
# MAGIC FROM workspace.default.silver_inscricoes
# MAGIC WHERE TIPO_PESSOA = 'Pessoa jurídica'
# MAGIC GROUP BY LEFT(CPF_CNPJ, 10)
# MAGIC ORDER BY SUM(VALOR_CONSOLIDADO) DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH base AS (
# MAGIC     SELECT
# MAGIC         LEFT(CPF_CNPJ, 10) AS raiz_cnpj,
# MAGIC         NOME_DEVEDOR,
# MAGIC         TIPO_SITUACAO_INSCRICAO,
# MAGIC         VALOR_CONSOLIDADO
# MAGIC     FROM workspace.default.silver_inscricoes
# MAGIC     WHERE TIPO_PESSOA = 'Pessoa jurídica'
# MAGIC ),
# MAGIC top20 AS (
# MAGIC     SELECT raiz_cnpj, MAX(NOME_DEVEDOR) AS nome, SUM(VALOR_CONSOLIDADO) AS divida_total
# MAGIC     FROM base
# MAGIC     GROUP BY raiz_cnpj
# MAGIC     ORDER BY divida_total DESC
# MAGIC     LIMIT 20
# MAGIC )
# MAGIC SELECT
# MAGIC     t.nome,
# MAGIC     b.TIPO_SITUACAO_INSCRICAO AS situacao,
# MAGIC     ROUND(100 * SUM(b.VALOR_CONSOLIDADO) / t.divida_total, 1) AS perc_valor,
# MAGIC     CASE
# MAGIC         WHEN 100 * SUM(b.VALOR_CONSOLIDADO) / t.divida_total < 5 THEN ''
# MAGIC         ELSE CONCAT(TRANSLATE(FORMAT_NUMBER(100 * SUM(b.VALOR_CONSOLIDADO) / t.divida_total, 0), ',.', '.,'), '%')
# MAGIC     END AS perc_br,
# MAGIC     t.divida_total
# MAGIC FROM base b
# MAGIC JOIN top20 t ON b.raiz_cnpj = t.raiz_cnpj
# MAGIC GROUP BY t.nome, t.divida_total, b.TIPO_SITUACAO_INSCRICAO
# MAGIC ORDER BY t.divida_total DESC, situacao

# COMMAND ----------

# MAGIC %md
# MAGIC **Resultado:**
# MAGIC
# MAGIC | # | Devedor | Dívida (R$ bi) | % cobrança | % benefício fiscal | % garantia | % suspenso | Inscrição mais antiga |
# MAGIC |---|---|---|---|---|---|---|---|
# MAGIC | 1 | Petrobras | 87,97 | 0,1% | 46,7% | 52,0% | 1,2% | 19/08/2002 |
# MAGIC | 2 | Vale | 55,26 | 0,0% | 89,4% | 10,6% | 0,0% | 17/11/1995 |
# MAGIC | 3 | Carital Brasil (falido) | 35,08 | 99,3% | 0,0% | 0,0% | 0,7% | 02/02/2005 |
# MAGIC | 4 | Banco Santander | 25,04 | 1,0% | 47,5% | 49,1% | 2,5% | 04/07/1997 |
# MAGIC | 5 | Ambev | 16,85 | 0,0% | 0,5% | 98,2% | 1,2% | 09/02/1995 |
# MAGIC | 6 | Indústrias de Papel R. Ramenzoni | 13,42 | 99,6% | 0,4% | 0,0% | 0,0% | 31/03/1993 |
# MAGIC | 7 | PPL Participações (falido) | 10,75 | 100,0% | 0,0% | 0,0% | 0,0% | 21/07/2006 |
# MAGIC | 8 | Companhia Siderúrgica Nacional | 10,54 | 0,0% | 0,7% | 79,0% | 20,4% | 17/04/2008 |
# MAGIC | 9 | RRJ Credit | 9,76 | 100,0% | 0,0% | 0,0% | 0,0% | 30/04/2025 |
# MAGIC | 10 | Duagro Administração e Participações | 9,16 | 100,0% | 0,0% | 0,0% | 0,0% | 03/07/2003 |
# MAGIC | 11 | Banco Bradesco | 9,09 | 0,0% | 26,2% | 73,8% | 0,1% | 03/07/2015 |
# MAGIC | 12 | Tinto Holding (massa falida) | 9,08 | 88,7% | 0,0% | 11,3% | 0,0% | 13/02/2004 |
# MAGIC | 13 | Viação Aérea São Paulo | 8,76 | 68,8% | 0,0% | 0,0% | 31,2% | 19/03/1993 |
# MAGIC | 14 | Base Engenharia (falido) | 7,76 | 100,0% | 0,0% | 0,0% | 0,0% | 07/08/2006 |
# MAGIC | 15 | Samarco Mineração | 7,67 | 0,0% | 41,6% | 21,5% | 36,9% | 11/05/1995 |
# MAGIC | 16 | Unilever Brasil Industrial | 7,40 | 0,0% | 0,0% | 100,0% | 0,0% | 09/08/1996 |
# MAGIC | 17 | Ragi Refrigerantes | 6,34 | 91,1% | 0,0% | 0,0% | 8,9% | 11/10/2005 |
# MAGIC | 18 | Axia Energia | 6,27 | 0,0% | 0,0% | 100,0% | 0,0% | 30/07/2015 |
# MAGIC | 19 | Viação Aérea Riograndense (falida) | 6,09 | 100,0% | 0,0% | 0,0% | 0,0% | 29/12/1975 |
# MAGIC | 20 | Ympactus Comercial | 5,83 | 0,0% | 100,0% | 0,0% | 0,0% | 01/09/2014 |
# MAGIC
# MAGIC **Conclusões:**
# MAGIC
# MAGIC - **Os 20 maiores devedores empresariais somam R$ 348,1 bilhões, 11,8% de todo o estoque.**
# MAGIC - **O topo se divide em dois grupos quase sem meio-termo.** Dez devedores têm praticamente nada em cobrança: suas dívidas estão parceladas, garantidas ou suspensas. Somam R$ 231,9 bilhões (66,6% do top 20) e incluem grandes companhias em operação, como Petrobras, Vale, Santander, Ambev, CSN e Bradesco. Os outros dez estão quase inteiramente em cobrança e somam R$ 116,2 bilhões.
# MAGIC - **A garantia pesa tanto quanto a negociação entre as grandes companhias.** Ambev, Unilever e Axia Energia têm a dívida integralmente ou quase integralmente garantida; na Petrobras, na CSN e no Bradesco, a garantia passa da metade do valor, e no Santander fica em 49,1%. Para esse grupo, o desfecho depende mais do resultado das disputas judiciais do que de ações de cobrança.
# MAGIC - **A dívida em cobrança do topo é dominada por empresas falidas e débitos antigos.** Dos dez devedores em cobrança, cinco têm falência indicada no próprio nome registrado (R$ 68,8 bilhões), e quase todos têm inscrições de mais de 15 anos. A recuperação desse bloco depende do andamento dos processos falimentares.
# MAGIC - **Exceção relevante:** a RRJ Credit tem R$ 9,76 bilhões em apenas 4 inscrições, todas a partir de abril de 2025 e integralmente em cobrança. É um caso recente de alto valor, exatamente o perfil que a análise 6.5 aponta como a janela de maior chance de recuperação.
# MAGIC - **Comparação com a versão anterior:** a primeira versão concluía que o topo em cobrança era formado por massas falidas e que os créditos viáveis estavam em parcelamentos. Os dados corrigidos confirmam a primeira parte, mas mostram que a garantia, e não só o parcelamento, é o principal instrumento entre as grandes companhias.
# MAGIC
# MAGIC **Limitação:** o indicador de falência se baseia em termos presentes no nome registrado e não captura todos os casos.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Memória de cálculo e regras de negócio
# MAGIC
# MAGIC ### Unidades de análise
# MAGIC
# MAGIC - **Inscrição:** cada dívida inscrita em dívida ativa, identificada pelo número de inscrição. É a unidade de todas as métricas de valor.
# MAGIC - **Devedor:** nas análises 6.3 e 6.8, empresas são agrupadas pela raiz do CNPJ (8 primeiros dígitos), para que matriz e filiais contem como um único devedor; pessoas físicas são agrupadas por CPF mascarado e nome. Na análise 6.2.1 e na tabela gold_uf_pessoa, o devedor é identificado pelo CPF ou CNPJ completo e pelo nome, porque cada estabelecimento tem sua própria UF e seus próprios processos.
# MAGIC
# MAGIC ### Regras de negócio
# MAGIC
# MAGIC 1. **Apenas o devedor principal entra nas métricas de valor.** A base repete a inscrição para cada corresponsável e devedor solidário, sempre com o valor integral. Somar todas as linhas resultaria em R$ 6,83 trilhões, contra o estoque real de R$ 2,96 trilhões.
# MAGIC 2. **Valor da dívida:** campo VALOR_CONSOLIDADO, que corresponde ao débito originário atualizado e somado aos encargos e acréscimos legais, conceito definido no art. 1º, § 2º, da Portaria MF nº 75/2012.
# MAGIC 3. **Data de referência:** 10/07/2026, a data de inscrição mais recente da base. É a data usada para calcular a idade das dívidas.
# MAGIC 4. **Ajuizamento:** inscrição com INDICADOR_AJUIZADO igual a "SIM".
# MAGIC 5. **Situação:** as 5 categorias de TIPO_SITUACAO_INSCRICAO (em cobrança, benefício fiscal, garantia, suspenso por decisão judicial e em negociação), que agrupam as situações detalhadas do campo SITUACAO_INSCRICAO.
# MAGIC 6. **Faixas de valor:** limites de R$ 10 mil, R$ 100 mil, R$ 1 milhão, R$ 15 milhões e R$ 100 milhões, escolhidos para separar ordens de grandeza. Cada faixa inclui o limite inferior e exclui o superior. Na análise 6.3 as faixas se aplicam à dívida total do devedor; na 6.7, ao valor de cada inscrição.
# MAGIC 7. **Faixas de idade:** menos de 1 ano, 1 a 5, 5 a 10, 10 a 15, 15 a 20 e mais de 20 anos. Idade = dias entre a data de inscrição e a data de referência, divididos por 365,25 e arredondados a uma casa decimal.
# MAGIC 8. **Limite de ajuizamento (análise 6.2.1):** R$ 20 mil, conforme o art. 1º, II, da Portaria MF nº 75/2012. O período "antes" ou "depois" da portaria é definido pela data de inscrição, com corte em 29/03/2012, data de publicação no Diário Oficial. A dívida total do devedor soma todas as suas inscrições ajuizadas.
# MAGIC 9. **UF sem informação:** 245 inscrições com UF "Si" foram mantidas na base. O código original foi preservado na camada silver e, na camada gold, esses registros aparecem como "Sem informação".
# MAGIC 10. **Indício de falência ou recuperação judicial (análise 6.8):** nome do devedor contendo os termos FALID, RECUPERACAO JUDICIAL ou LIQUIDACAO.
# MAGIC
# MAGIC ### Métricas
# MAGIC
# MAGIC | Métrica | Cálculo |
# MAGIC |---|---|
# MAGIC | Inscrições | Contagem de linhas da tabela silver_inscricoes |
# MAGIC | Valor (R$ bilhões) | Soma do valor consolidado, dividida por 1 bilhão |
# MAGIC | % do valor | Valor do grupo dividido pelo valor total do recorte, vezes 100 |
# MAGIC | Valor médio por inscrição | Valor do grupo dividido pela quantidade de inscrições |
# MAGIC | Valor mediano | Valor que divide as inscrições do grupo ao meio, calculado com a função PERCENTILE_APPROX do Spark. É uma aproximação adequada para bases com milhões de linhas, mas o resultado pode variar alguns reais entre execuções; os valores do texto correspondem à última execução de cada consulta |
# MAGIC | % no período (6.2.1) | Inscrições do enquadramento divididas pelo total de inscrições ajuizadas do mesmo período |
# MAGIC | % com corresponsável | Inscrições com pelo menos um corresponsável divididas pelo total de inscrições da faixa |
# MAGIC | Média de corresponsáveis | Média calculada apenas entre as inscrições que têm corresponsável, para separar quantas dívidas têm corresponsável de quantas pessoas respondem por elas |
# MAGIC | Devedores pessoa física | Aproximação: o CPF mascarado pela PGFN é combinado com o nome para reduzir o risco de contar duas pessoas como uma |
# MAGIC
# MAGIC ### O que cada gráfico revela
# MAGIC
# MAGIC | Análise | Gráfico | O que revela |
# MAGIC |---|---|---|
# MAGIC | 6.1 | Estoque por situação (R$ bi) | 62,8% do estoque está em cobrança, sem negociação, garantia ou suspensão |
# MAGIC | 6.2 | Valor por situação e ajuizamento (R$ bi) | A cobrança ajuizada sem garantia é o maior bloco da carteira, com R$ 1,44 trilhão |
# MAGIC | 6.2.1 | Funil do ajuizamento x limite de R$ 20 mil | Metade das ajuizadas tem valor individual até R$ 20 mil, mas só 1,0% continua abaixo do limite somando a dívida do devedor |
# MAGIC | 6.3 | Participação no valor por faixa de devedor (%) | 0,04% dos devedores concentram 51,3% do valor |
# MAGIC | 6.4 | 10 maiores estados: empresas x pessoas físicas (R$ bi) | A dívida é empresarial em todos os estados e se concentra em SP e RJ |
# MAGIC | 6.5 | Situação do estoque por idade da dívida (%) | Depois dos 15 anos, a parcela em cobrança sobe para cerca de 80% e a garantia praticamente desaparece |
# MAGIC | 6.6 | 15 receitas com maior estoque (R$ bi) | IRPJ, COFINS e CSLL somam 47,2% do valor |
# MAGIC | 6.7 | Média de corresponsáveis por faixa de valor | O número médio de corresponsáveis sobe de 1,1 para 5,2 conforme o valor da inscrição cresce |
# MAGIC | 6.8 | 20 maiores devedores: situação da dívida (%) | O topo se divide entre grandes companhias negociadas ou garantidas e empresas em cobrança, várias falidas |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Resumo executivo e recomendações
# MAGIC
# MAGIC ### Mensagem central
# MAGIC
# MAGIC O estoque da base SIDA soma R$ 2,96 trilhões em 30,8 milhões de inscrições, mas esse número esconde duas carteiras muito diferentes. De um lado, cerca de 20 mil devedores concentram 71% do valor e pedem atuação individualizada. Do outro, mais de 7 milhões de devedores com dívidas abaixo de R$ 100 mil somam menos de 3% do valor e só podem ser tratados com cobrança em massa. Somado a isso, um terço do estoque tem mais de 10 anos, e a dívida que não é negociada ou garantida nos primeiros anos tende a permanecer em cobrança sem solução.
# MAGIC
# MAGIC ### Principais achados
# MAGIC
# MAGIC 1. **62,8% do estoque (R$ 1,86 trilhão) está em cobrança**, sem negociação, garantia ou suspensão (6.1).
# MAGIC 2. **A cobrança ajuizada sem garantia é o maior bloco da carteira:** R$ 1,44 trilhão, 48,7% do estoque (6.2).
# MAGIC 3. **O limite de ajuizamento é compatível com a soma por devedor:** das inscrições ajuizadas com valor individual até R$ 20 mil, quase todas pertencem a devedores que, somados, passam do limite; só 1,0% das ajuizadas fica abaixo dele (6.2.1).
# MAGIC 4. **A concentração é extrema:** 19.804 devedores (0,24%) respondem por 71% do valor, e 3.270 (0,04%) por 51,3% (6.3).
# MAGIC 5. **A dívida é empresarial em todos os estados** e se concentra em SP e RJ (55,4%), em parte pela localização das sedes (6.4).
# MAGIC 6. **A dívida envelhece sem solução:** acima de 15 anos estão R$ 524,22 bilhões (17,7% do valor), com cerca de 80% em cobrança e garantia de apenas 3,5% na faixa de mais de 20 anos (6.5).
# MAGIC 7. **IRPJ, COFINS e CSLL somam 47,2% do valor**, enquanto o Simples Nacional tem o maior número de inscrições e valores menores (6.6).
# MAGIC 8. **Quanto maior a dívida, mais pessoas respondem por ela:** a média de corresponsáveis sobe de 1,1 para 5,2 conforme o valor da inscrição cresce (6.7).
# MAGIC 9. **Os 20 maiores devedores empresariais somam R$ 348,1 bilhões**, divididos entre grandes companhias com dívida negociada ou garantida (R$ 231,9 bilhões) e empresas em cobrança, cinco delas com falência indicada no nome (R$ 68,8 bilhões) (6.8).
# MAGIC
# MAGIC ### Recomendações e decisões de negócio
# MAGIC
# MAGIC **1. Separar a carteira em dois modelos de cobrança.**
# MAGIC - **Decisão:** atuação individualizada, com equipe dedicada, para os devedores acima de R$ 15 milhões; cobrança automatizada e em massa, como protesto de CDA e transação por adesão, para os devedores abaixo de R$ 100 mil.
# MAGIC - **Base:** 6.3 e 6.2. Os 19.804 maiores devedores concentram R$ 2,10 trilhões; os 7,3 milhões menores somam 2,8% do valor.
# MAGIC - **Indicador:** valor recuperado por faixa de devedor, a cada trimestre.
# MAGIC
# MAGIC **2. Agir cedo sobre a dívida recente.**
# MAGIC - **Decisão:** priorizar instrumentos de cobrança e negociação no primeiro ano após a inscrição, antes que o devedor perca patrimônio.
# MAGIC - **Base:** 6.5. 74,5% do valor inscrito no último ano (R$ 282,11 bilhões na faixa) está em cobrança, sem negociação nem garantia.
# MAGIC - **Indicador:** percentual do valor inscrito no ano que é negociado, garantido ou pago em até 12 meses.
# MAGIC
# MAGIC **3. Tratar a dívida antiga como um bloco separado.**
# MAGIC - **Decisão:** fazer triagem das inscrições com mais de 15 anos (situação do devedor, risco de prescrição intercorrente, existência de bens) e direcionar os casos de difícil recuperação para transação ou para baixa, separando nos relatórios o valor contábil do valor com perspectiva real de recuperação.
# MAGIC - **Base:** 6.5. São 701.445 inscrições e R$ 524,22 bilhões, com cerca de 80% em cobrança.
# MAGIC - **Indicador:** valor do estoque acima de 15 anos e sua participação no total, acompanhados ao longo dos trimestres.
# MAGIC
# MAGIC **4. Usar os corresponsáveis nas execuções sem garantia.**
# MAGIC - **Decisão:** nas dívidas acima de R$ 15 milhões em execução sem garantia, incluir a pesquisa patrimonial dos corresponsáveis na estratégia de cobrança.
# MAGIC - **Base:** 6.7 e 6.2. Nessas faixas, mais de um terço das inscrições tem corresponsável, com média de 4,2 a 5,2 pessoas, e a cobrança ajuizada sem garantia soma R$ 1,44 trilhão.
# MAGIC - **Indicador:** valor recuperado em execuções com redirecionamento a corresponsáveis.
# MAGIC
# MAGIC **5. Monitorar a adimplência dos benefícios fiscais.**
# MAGIC - **Decisão:** acompanhar a carteira parcelada ou transacionada como receita condicionada, com alerta para rescisões.
# MAGIC - **Base:** 6.1. R$ 603,63 bilhões (20,4% do estoque) dependem de o devedor continuar pagando.
# MAGIC - **Indicador:** taxa de rescisão por programa de parcelamento ou transação (dado que não está nesta base e exigiria outra fonte).
# MAGIC
# MAGIC **6. Acompanhar de perto os grandes devedores falidos.**
# MAGIC - **Decisão:** concentrar a atuação sobre esse grupo na habilitação e no acompanhamento dos processos falimentares, sem esperar recuperação por ações de cobrança comuns.
# MAGIC - **Base:** 6.8. Cinco dos 20 maiores devedores têm falência indicada no nome e somam R$ 68,8 bilhões, quase todos com inscrições de mais de 15 anos.
# MAGIC - **Indicador:** valor recuperado em processos falimentares sobre o valor habilitado.
# MAGIC
# MAGIC **7. Desenhar programas de negociação por origem da dívida.**
# MAGIC - **Decisão:** programas em massa para o Simples Nacional (3,6 milhões de inscrições, valores menores) e atuação individualizada sobre os grandes devedores de IRPJ, CSLL e IPI. No IPI, poucas inscrições concentram valores altos; no IRPJ e na CSLL, o valor vem de mais de 1,7 milhão de inscrições cada, e o foco deve estar nos devedores de maior valor identificados na 6.3.
# MAGIC - **Base:** 6.6. IRPJ, COFINS e CSLL somam 47,2% do valor; o IPI tem a maior mediana da lista.
# MAGIC - **Indicador:** adesão e valor negociado por programa e por receita.
# MAGIC
# MAGIC ### Nota de escopo
# MAGIC
# MAGIC As recomendações partem exclusivamente dos dados públicos da base SIDA. Não consideram custos operacionais, capacidade das equipes nem a estratégia já adotada pela PGFN, que usa vários desses instrumentos. O objetivo é mostrar como os dados podem orientar a priorização, e não avaliar a gestão atual.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Limitações, melhorias e próximos passos
# MAGIC
# MAGIC ### Limitações
# MAGIC
# MAGIC 1. **Recorte parcial da dívida ativa:** a análise cobre apenas a base SIDA. A dívida previdenciária e a do FGTS ficam de fora, então os resultados não representam a dívida ativa da União como um todo.
# MAGIC 2. **Fotografia de um único momento:** os dados são do 2º trimestre de 2026. Sem séries históricas, não é possível medir quanto do estoque foi recuperado, quanto entrou ou quanto saiu ao longo do tempo.
# MAGIC 3. **Ausência de dados de pagamento:** a base mostra o estoque, mas não o que foi efetivamente recuperado. As conclusões sobre recuperabilidade são indiretas, baseadas em situação, idade e garantia.
# MAGIC 4. **Valores atualizados:** o valor consolidado inclui juros e encargos até a data de referência. Dívidas antigas aparecem maiores do que eram na inscrição ou no ajuizamento.
# MAGIC 5. **Data do ajuizamento indisponível:** a base informa se a inscrição foi ajuizada, mas não quando. Por isso, a análise da portaria (6.2.1) usa a data de inscrição como aproximação.
# MAGIC 6. **CPF mascarado:** a contagem de devedores pessoa física combina CPF parcial e nome e pode juntar ou separar pessoas incorretamente em casos raros.
# MAGIC 7. **UF do domicílio fiscal:** a geografia reflete onde estão as sedes, e não onde a atividade econômica acontece.
# MAGIC 8. **Indicador de falência aproximado:** baseado em termos no nome do devedor, não captura empresas falidas que não tiveram o nome alterado na base.
# MAGIC 9. **Medianas aproximadas:** calculadas com PERCENTILE_APPROX, podem variar alguns reais entre execuções.
# MAGIC
# MAGIC ### Oportunidades de melhoria
# MAGIC
# MAGIC 1. **Série histórica:** carregar os trimestres anteriores publicados pela PGFN para medir entrada, saída e envelhecimento do estoque ao longo do tempo.
# MAGIC 2. **Bases complementares da PGFN:** incluir a dívida previdenciária e a do FGTS para ter a visão completa da dívida ativa da União.
# MAGIC 3. **Situação cadastral das empresas:** cruzar a raiz do CNPJ com os dados abertos do CNPJ da Receita Federal, para identificar empresas baixadas, inaptas ou falidas com mais precisão do que pelo nome.
# MAGIC 4. **Contexto econômico por estado:** comparar a participação de cada UF no estoque com sua participação no PIB, para separar a concentração esperada da concentração fora do padrão.
# MAGIC 5. **Mediana exata:** testar o cálculo exato da mediana nos grupos menores, para eliminar a variação entre execuções onde o custo de processamento permitir.
# MAGIC 6. **Testes de qualidade automatizados:** transformar as conferências manuais (contagem de linhas, valores inválidos, inscrições sem UF) em verificações que rodem a cada carga.
# MAGIC 7. **Automação da carga:** agendar a ingestão trimestral com Databricks Jobs, para que o projeto se atualize a cada nova publicação da PGFN.
# MAGIC
# MAGIC ### Próximos passos
# MAGIC
# MAGIC 1. Publicar o projeto no GitHub, com README, notebook em HTML, DBC e código-fonte.
# MAGIC 2. Construir um dashboard de apresentação no Databricks, com os indicadores principais e os gráficos das análises.
# MAGIC 3. Incorporar a série histórica, começando pelos trimestres de 2025, para transformar a fotografia em acompanhamento.