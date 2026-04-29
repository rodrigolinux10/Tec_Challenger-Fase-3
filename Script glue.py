import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from itertools import chain
sc = SparkContext()

#====================================
# Definição e carregamento do Job
#====================================

try:
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
except:
    args = {'JOB_NAME': 'job_covid_pnad'}
    
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


#========================================
# DICIONÁRIOS DE RENOMEAÇÃO E CATEGORIAS
#========================================


# Colunas selecionadas do PNAD
colunas_desejadas = [
    "A002", "A003", "A004", "B005", "B006",
    "B0011", "B0012", "B0019", "B00111",
    "B0101", "B0106", "B008", "B009B",
    "B011", "C001", "C013", "C01012", "D0051"
]

# Dicionário de Renomeação
dicionario_colunas = {
    "A002": "idade",           # Verificado
    "A003": "sexo",            # Verificado
    "A004": "raca_cor",        # Verificado
    "B005": "foi_internado",   # Verificado
    "B006": "uso_respirador",  # Verificado
    "B0011": "sintoma_febre",  # Verificado
    "B0012": "sintoma_tosse",  # Verificado
    "B0019": "sintoma_perda_olfato_paladar",  # Modificado, estava como B00111
    "B00111": "sintoma_falta_ar",             # Modificado, estava como sintoma_fadiga
    "B0101": "ja_teve_diabetes",   # Consertado
    "B0106": "ja_teve_cancer",     # Consertado
    "B008": "fez_algum_teste_de_covid",        # Consertado
    "B009B": "resultado_teste_covid_caso_feito",  # Consertado
    "B011": "o_que_fez_apos_o_teste",          # Consertado
    "C001": "trabalhou_semana_passada",        # Verificado
    "C013": "trabalho_remoto",                 # Verificado
    "C01012": "rendimento_mensal",             # Verificado
    "D0051": "recebeu_auxilio_emergencial",    # Verificado
}

#========================================================
# Dicionário de Categorias com tratamento de Nulos
#========================================================

mapeamento_categorias = {
    "sexo":          {1: "Homem", 2: "Mulher"},
    "raca_cor":      {1: "Branca", 2: "Preta", 3: "Amarela", 4: "Parda", 5: "Indígena", 9: "Ignorado"},
    "foi_internado": {1: "Sim", 2: "Não", 9: "Ignorado", "vazio": "Não aplicável"},
    "uso_respirador":{1: "Sim", 2: "Não", 9: "Ignorado", "vazio": "Não aplicável"},
    "sintoma_febre":               {1: "Sim", 2: "Não", 3: "Não sabe", 9: "Ignorado"},
    "sintoma_tosse":               {1: "Sim", 2: "Não", 3: "Não sabe", 9: "Ignorado"},
    "sintoma_falta_ar":            {1: "Sim", 2: "Não", 3: "Não sabe", 9: "Ignorado"},
    "sintoma_perda_olfato_paladar":{1: "Sim", 2: "Não", 3: "Não sabe", 9: "Ignorado"},
    "ja_teve_diabetes": {1: "Positivo", 2: "Negativo", 9: "Ignorado", "vazio": "Não fez o teste"},
    "ja_teve_cancer":   {1: "Positivo", 2: "Negativo", 9: "Ignorado", "vazio": "Não fez o teste"},
    "fez_algum_teste_de_covid": {1: "Positivo", 2: "Negativo", "vazio": "Não fez o teste", 9: "Ignorado"},
    "resultado_teste_covid_caso_feito": {
        1: "Sim", 2: "Não", 3: "Inconclusivo", 9: "Ignorado", "vazio": "Não aplicável"
    },
    "trabalhou_semana_passada": {1: "Sim", 2: "Não", "vazio": "Não aplicável"},
    "trabalho_remoto":          {1: "Sim", 2: "Não", "vazio": "Não aplicável"},
    "recebeu_auxilio_emergencial": {1: "Sim", 2: "Não", "vazio": "Não recebeu/Não aplicável"},
}

print("✅ Dicionários carregados com sucesso!")

# Definição dos caminhos
bucket_name = "x" # nome do seu bucket aqui
caminho_input = f"s3://{bucket_name}/raw/"

#=============================
# Lista dos arquivos e bases
#=============================

tabelas = [
    "PNAD_COVID_092020.csv",
    "PNAD_COVID_102020.csv",
    "PNAD_COVID_112020.csv"
]

#=====================================================
# Dicionário para armazenar os DataFrames carregados
#=====================================================

dfs_originais = {}

for tabela in tabelas:
    path = caminho_input + tabela
    print(f"Lendo arquivo: {path}")
    
    # Leitura direta do S3 para Spark DataFrame
    # O Glue utiliza o protocolo s3:// nativamente
    df = spark.read.option("header", "true") \
                   .option("sep", ",") \
                   .option("inferSchema", "true") \
                   .csv(path)
    
    dfs_originais[tabela] = df

print("Todos os arquivos foram carregados com sucesso para o Glue!")

#================================================================
# --- BLOCO 1: APLICAÇÃO DE DICIONÁRIOS COM CAST PARA INT ---
#================================================================

dfs_transformados = {}

for tabela, df in dfs_originais.items():
    print(f"Transformando dados de: {tabela}")
    
    # 1. Seleciona e renomeia as colunas
    df_sel = df.select([F.col(c).alias(dicionario_colunas.get(c, c)) for c in colunas_desejadas])
    
    # 2. Aplica o mapeamento de categorias
    for col_name, mapping in mapeamento_categorias.items():
        if col_name in df_sel.columns:
            
          
            # Converte a coluna para Integer antes de procurar no dicionário
            df_sel = df_sel.withColumn(col_name, F.col(col_name).cast("int"))
            
            # Prepara o mapa (removendo a chave "vazio" que é string para não quebrar o map de ints)
            mapping_pure = {k: v for k, v in mapping.items() if k != "vazio"}
            mapping_expr = F.create_map([F.lit(x) for x in chain(*mapping_pure.items())])
            
            # Aplica o De/Para
            df_sel = df_sel.withColumn(col_name, mapping_expr[F.col(col_name)])
            
            # 3. Tratamento para valores que não estavam no mapeamento (vazio/null)
            if "vazio" in mapping:
                df_sel = df_sel.withColumn(
                    col_name, 
                    F.coalesce(F.col(col_name), F.lit(mapping["vazio"]))
                )

    dfs_transformados[tabela] = df_sel

print(" Todos os DataFrames foram transformados com sucesso (Inteiros corrigidos)!")


#===========================================
# Dicionário de labels para os meses 
#===========================================

meses_label = {
    "092020": "Setembro",
    "102020": "Outubro",
    "112020": "Novembro"
}

for tabela, df_sel in dfs_transformados.items():
    # 1. Extração do mês para a camada Gold
    try:
        mes_ref = tabela.split('_')[2].split('.')[0]
        label_mes = meses_label.get(mes_ref, mes_ref)
    except:
        mes_ref = "processado"
        label_mes = "Desconhecido"

    # 2. Escrita na Camada Silver (Parquet)
    # No Glue, indicamos apenas o diretório. O Spark gera os arquivos lá dentro.
    caminho_silver = f"s3://{bucket_name}/silver/{tabela.replace('.csv', '')}"
    print(f"Gravando Silver em: {caminho_silver}")
    
    df_sel.write.mode("overwrite").parquet(caminho_silver)

    # 3. Escrita na Camada Gold (com a coluna de mês)
    df_gold = df_sel.withColumn("mes_referencia", F.lit(label_mes))
    
    caminho_gold = f"s3://{bucket_name}/gold/pnad_covid_{mes_ref}"
    print(f"Gravando Gold em: {caminho_gold}")
    
    df_gold.write.mode("overwrite").parquet(caminho_gold)

print("Processo concluído: Camadas Silver e Gold atualizadas no S3.")

#==================================================
# --- BLOCO DE AGREGAÇÃO TOTAL CONSOLIDADA ---
#==================================================

# 1. Unificar todos os DataFrames que já foram transformados
df_total = None
for df in dfs_transformados.values():
    if df_total is None:
        df_total = df
    else:
        df_total = df_total.union(df)

# 2. Agregação: Total de pessoas que fizeram o teste (independente de mês ou resultado)
agregacao_total = df_total.groupBy("fez_algum_teste_de_covid") \
    .agg(F.count("*").alias("total_pessoas")) \
    .orderBy(F.desc("total_pessoas"))

# 3. Exibir o resultado consolidado no log
print("📊 Total acumulado de realização de testes (Todos os meses):")
agregacao_total.show()

# 4. Salvar apenas o resultado desta agregação na Gold
caminho_agregacao_total = f"s3://{bucket_name}/gold/agregacao_total_testes_covid"
agregacao_total.repartition(1).write.mode("overwrite").parquet(caminho_agregacao_total)

print(f"✅ Agregação consolidada salva com sucesso em: {caminho_agregacao_total}")

# --- Gold 6: Focalização e Eficácia do Auxílio Emergencial ---

print("Iniciando cálculo consolidado de focalização...")

#  DataFrame fd_total contém a união de todos os meses


df_analise = df_total 

gold_focalizacao_total = df_analise.agg(
    F.count('idade').alias('total_pessoas'),

    # Eficácia: Filtramos quem NÃO trabalhou e SIM recebeu auxílio
    F.sum(
        F.when(
            (F.col('trabalhou_semana_passada') == 'Não') & 
            (F.col('recebeu_auxilio_emergencial') == 'Sim'), 1
        ).otherwise(0)
    ).alias('sem_trabalho_com_auxilio'),

    # Erro de Exclusão: Quem NÃO trabalhou e NÃO recebeu auxílio
    # Importante: Checamos se é 'Não' ou se o campo está como 'Não recebeu/Não aplicável' (conforme seu dict)
    F.sum(
        F.when(
            (F.col('trabalhou_semana_passada') == 'Não') & 
            (F.col('recebeu_auxilio_emergencial').contains('Não')), 1
        ).otherwise(0)
    ).alias('sem_trabalho_sem_auxilio_vulneraveis'),

    # Renda Média
    F.avg(
        F.when(F.col('recebeu_auxilio_emergencial') == 'Sim', F.col('rendimento_mensal'))
    ).alias('rendimento_medio_com_auxilio'),

    F.avg(
        F.when(F.col('recebeu_auxilio_emergencial').contains('Não'), F.col('rendimento_mensal'))
    ).alias('rendimento_medio_sem_auxilio')
)

# Cálculo da Taxa de Cobertura
gold_focalizacao_total = gold_focalizacao_total.withColumn(
    'taxa_cobertura_vulneraveis_percentual',
    F.when(
        (F.col('sem_trabalho_com_auxilio') + F.col('sem_trabalho_sem_auxilio_vulneraveis')) > 0,
        (F.col('sem_trabalho_com_auxilio') / 
         (F.col('sem_trabalho_com_auxilio') + F.col('sem_trabalho_sem_auxilio_vulneraveis'))) * 100
    ).otherwise(0)
)

print(" Resultado da Focalização:")

#================================================================
# --- Gold 7: Correlação de Sintomas vs Resultado de Covid ---
#================================================================

print("Iniciando análise de sintomas por resultado de teste...")

# 1. Definimos a lista de sintomas que mapeamos no dicionário
lista_sintomas = [
    "sintoma_febre", 
    "sintoma_tosse", 
    "sintoma_falta_ar", 
    "sintoma_perda_olfato_paladar"
]

# 2. Filtramos apenas quem tem um resultado de teste válido (Sim ou Não)
# No seu dicionário, 'resultado_teste_covid_caso_feito' mapeia 1 para 'Sim' e 2 para 'Não'
df_testados = df_total.filter(
    F.col("resultado_teste_covid_caso_feito").isin(["Sim", "Não"])
)

# 3. Criamos a agregação pivotada ou por grupos
# Vamos calcular quantos 'Sim' cada sintoma teve, agrupado pelo resultado do teste
analise_sintomas = df_testados.groupBy("resultado_teste_covid_caso_feito").agg(
    F.count("*").alias("total_no_grupo"),
    *[F.sum(F.when(F.col(s) == "Sim", 1).otherwise(0)).alias(f"com_{s}") for s in lista_sintomas]
)

# 4. Calculamos a representatividade (Percentual) para facilitar a comparação
for s in lista_sintomas:
    col_nome = f"com_{s}"
    pct_nome = f"pct_{s}"
    analise_sintomas = analise_sintomas.withColumn(
        pct_nome, 
        (F.col(col_nome) / F.col("total_no_grupo")) * 100
    )

print("📊 Ranking de Sintomas por Confirmação de Covid:")
analise_sintomas.select(
    "resultado_teste_covid_caso_feito",
    "total_no_grupo",
    "pct_sintoma_febre",
    "pct_sintoma_tosse",
    "pct_sintoma_falta_ar",
    "pct_sintoma_perda_olfato_paladar"
).show()

#===============================
# 5. Salvando na camada Gold
#===============================

caminho_sintomas = f"s3://{bucket_name}/gold/analise_correlacao_sintomas"
analise_sintomas.repartition(1).write.mode("overwrite").parquet(caminho_sintomas)

print(f"✅ Análise de sintomas salva em: {caminho_sintomas}")
job.commit()