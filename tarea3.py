#Shirley Yurani Pereira Cubillos
from pyspark.sql import SparkSession, functions as F

# Inicializa la sesión de Spark
spark = SparkSession.builder.appName('Tarea3').getOrCreate()

# Define la ruta del archivo .csv en HDFS
file_path = 'hdfs://localhost:9000/Tarea3/datos.csv'

# Lee el archivo .csv
df = spark.read.format('csv').option('header','true').option('inferSchema', 'true').load(file_path)

#imprimimos el esquema
df.printSchema()

# Muestra las primeras filas del DataFrame
df.show()

# Contar valores nulos por cada columna
print('------- VALORES NULOS: --------')
df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# Contar filas duplicadas
print('------- VALORES DUPLICADOS: --------')
df.agg(F.countDistinct(*df.columns).alias('distinct_rows'), F.count('*').alias('total_rows')).show()

# Eliminar duplicados si los hay
print('-------ELIMINAR VALORES DUPLICADOSS: --------')
df = df.dropDuplicates()

# Eliminar filas con valores nulos
print('-------ELIMINAR FILAS CON VALORES NULOS: --------')
df = df.na.drop()

# Descripción estadística básica
print('-------ESTADISTICA BASICA: --------')
df.describe().show()

df.select("Age", "Years_of_Experience","Hours_Worked_Per_Week", "Number_of_Virtual_MeetingS").describe().show(truncate=False)

df.select("Work_Life_Balance_Rating","Social_Isolation_Rating", "Company_Support_for_Remote_Work").describe().show(truncate=False)

# Contar empleados por modalidad de trabajo
print("-------CANTIDAD DE EMPLEADOS POR MODALIDAD DE TRABAJO: ------")
df.groupBy('Work_Location').count().show()


# Contar empleados por nivel de estrés
print("-------CANTIDAD DE EMPLEADOS POR NIVEL DE ESTRES: ------")
df.groupBy('Stress_Level').count().show()


# Relacionar modalidad de trabajo con niveles de estrés
print("-------RELACION MODALIDAD DE TRABAJO CON NIVEL DE ESTRES: ------")
df.groupBy('Work_Location', 'Stress_Level').count().show()


# Relacionar modalidad de trabajo con condiciones de salud mental
print("-------RELACION MODALIDAD DE TRABAJO CON CONDICION DE SALUD MENTAL: ------")
df.groupBy('Work_Location', 'Mental_Health_Condition').count().show()

# Agrupar por niveles de estrés y balance entre vida y trabajo
print("-------RELACION NIVEL DE ESTRES CON BALANCE VIDA/TRABAJO: ------")
df.groupBy('Stress_Level', 'Work_Life_Balance_Rating').count().show()

# Agrupar por nivel de estrés y calcular estadísticas de horas trabajadas por semana
print("-------RELACION NIVEL DE ESTRES CON ESTADISTICAS DE HORAS TRABAJADAS POR SEMANA: ------")
df.groupBy('Stress_Level').agg(
    F.mean('Hours_Worked_Per_Week').alias('mean_hours_worked'),
    F.stddev('Hours_Worked_Per_Week').alias('std_hours_worked')
).show()

# Agrupar por satisfacción con el trabajo remoto y nivel de estrés
print("-------RELACION NIVEL DE ESTRES CON SATISFACCION DEL TRABAJO REMOTO: ------")
df.groupBy('Satisfaction_with_Remote_Work', 'Stress_Level').count().show()

# Agrupar por satisfacción con el trabajo remoto y condiciones de salud mental
print("-------RELACION SATISFACCION DEL TRABAJO REMOTO Y CONDICION DE SALUD MENTAL: ------")
df.groupBy('Satisfaction_with_Remote_Work', 'Mental_Health_Condition').count().show()
