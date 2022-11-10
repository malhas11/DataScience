---
title: "Distributed Data Analysis - EDA"
author: "Saad Malhas"
date: "20/04/2022"
---

#imports
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA
from sklearn.datasets import load_boston
import plotly.express as px

#read data file
df = pd.read_csv("C:\\Users\\.....location.....\\pollution_clean.csv", sep="\t")
df.head()

#drop columns
df2 = df
df2 = df2.drop(columns=['No', 'year', 'month', 'day', 'hour', 'wd', 'station'])

#  pca

features = ['PM2.5', 'SO2', 'CO', 'O3', 'TEMP', 'PRES', 'DEWP', 'RAIN', 'WSPM']

x = df2.loc[:, features].values
y = df2.loc[:,['PM10']].values

# standardization, which is calculated (value - mean) / sd 
x = StandardScaler().fit_transform(x)
pca = PCA(n_components=9)
principalComponents = pca.fit_transform(x)

principalDf = pd.DataFrame(data = principalComponents)
principalDf.head()

finalDf = pd.concat([principalDf, df[['PM10']]], axis = 1)
finalDf.head()

#scree plot
PC_values = np.arange(pca.n_components_) + 1

plt.plot(PC_values, pca.explained_variance_ratio_, 'o-', linewidth=2, color='red')
plt.title('Scree plot')
plt.xlabel('Principal Component')
plt.ylabel('variance explained')
plt.show()

print(pca.explained_variance_ratio_)
