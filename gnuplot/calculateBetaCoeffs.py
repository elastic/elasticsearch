import numpy as np
import matplotlib.pyplot as plt
import csv

data = []
xData = []
yData = []

with open('coeffData.dat', 'r') as csvfile:
    csvreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
    for row in csvreader:
        rowData = []
        for value in row:
            rowData.append(float(value))
        data.append(rowData)
        xData.append(rowData[0])
        yData.append(rowData[1])

print(data)

# input = np.array(data)
# m = np.shape(input)[0]
# X = np.matrix([np.ones(m), input[:,0]]).T
# y = np.matrix(input[:,1]).T
# betaHat = np.linalg.inv(X.T.dot(X)).dot(X.T).dot(y)
# print(betaHat)

p = np.polyfit(xData, yData, 7)
print(p)

# plt.figure(1)
# xx = np.linspace(0, 5, 2)
# yy = np.array(betaHat[0] + betaHat[1] * xx)
# poly = np.array(p[0] + p[1] * xx + p[2] * xx * xx + p[3] * xx * xx * xx)
# plt.plot(xx, yy.T, color='b')
# plt.plot(xx, poly.T, color='g')
# plt.scatter(input[:,0], input[:,1], color='r')
# plt.show()
