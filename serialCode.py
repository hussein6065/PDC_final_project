def generateMatrix(N):
        return [[0 for i in range(N)]for j in range(N)]


if(__name__ =="__main__"):

    matrixA = []

    with open('M.txt','r') as file:
        for line in file.readlines():
            matrixA.append([int(index) for index in line.strip().split(" ")])
    N = len(matrixA)
    
    matrixC = generateMatrix(N)
    count = 0 
    for i in range(N):
        for j in range(N):
            for k in range(N):
                count+=1
                matrixC[i][j] += matrixA[i][k] * matrixA[k][j]
                # print(count)
    
    # print(matrixC)

    print("the size of the matrix is: ",N)
