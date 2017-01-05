import numpy as np
import matplotlib.pyplot as plt
import csv
from math import log

for precision in range(25,26):

    data = []
    with open('coeffData' + str(precision) + '.dat', 'r') as csvfile:
        csvreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
        for row in csvreader:
            data.append([float(row[1]), float(row[2])])

    input = np.array(data)

    m = np.shape(input)[0]
    z = input[:,0]
    zl = np.log(z+1)
    M = np.matrix([z, zl, zl**2, zl**3, zl**4, zl**5, zl**6, zl**7]).T #, zl**8, zl**9, zl**10, zl**11, zl**12]).T
    y = np.matrix(input[:,1]).T
    p = np.linalg.inv(M.T.dot(M)).dot(M.T).dot(y)

    print(precision, 'coefficients: {', p.A[0][0], ',', p.A[1][0], ',', p.A[2][0], ',', p.A[3][0], ',', p.A[4][0], ',',
          p.A[5][0], ',', p.A[6][0], ',', p.A[7][0], '}') \
        #, ',', p.A[8][0], ',', p.A[9][0], ',', p.A[10][0], ',', p.A[11][0], ',', p.A[12][0], '}')

# plt.figure(1)
# zz = np.linspace(0, np.max(z))
# zzlData = []
# for zzi in zz:
#     zzlData.append(log(zzi + 1))
# zzl = np.array(zzlData)
# poly = np.array(p[0] * zz + p[1] * zzl + p[2] * zzl**2 + p[3] * zzl**3 + p[4] * zzl**4 + p[5] * zzl**5 + p[6] * zzl**6 + p[7] * zzl**7)
# plt.plot(zz, poly.T, color='g')
# plt.scatter(input[:,0], input[:,1], color='r')
# plt.show()
