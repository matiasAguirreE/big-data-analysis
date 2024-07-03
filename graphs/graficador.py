import pandas as pd
import plotly.express as px


df = pd.read_excel('resultados.xlsx')

print("Datos leídos correctamente")


df['Año'] = df['Año_Mes'].apply(lambda x: int(x.split('_')[0]))  
df['Mes'] = df['Año_Mes'].apply(lambda x: int(x.split('_')[1])) 

df = df.sort_values('Mes')


meses = {1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril', 5: 'Mayo', 6: 'Junio',
         7: 'Julio', 8: 'Agosto', 9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'}
df['Mes'] = df['Mes'].apply(lambda x: meses[x] if x in meses else x)

print("Datos ordenados correctamente")
print("Datos procesados correctamente")


fig = px.line(df, x="Mes", y="Porcentaje", color='Año', 
              title='Porcentaje de usos bicicletas en Barcelona por Año', 
              labels={'Mes': 'Mes', 'Porcentaje': 'Porcentaje de uso'})

print("Gráfico generado correctamente")
fig.write_image("bicicletas_barcelona_por_año.png")
fig.show()