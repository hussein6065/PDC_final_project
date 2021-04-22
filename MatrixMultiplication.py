import math
import mrs
import string
import sys

class MatrixMultiplication(mrs.MapReduce):
    # generates and Empty Matrix
    def generateMatrix(self,N):
        return [[0 for i in range(N)]for j in range(N)]

    # Partitions the matrix in row major
    def rowMajor(self,M,p,offset):
        i = 0
        while i<p:
            start = i*offset
            end = (i*offset) + offset
            yield M[start:end]
            i += 1
    # Partitions the matrix in colum major
    def columnMajor(self,M,p,offset):
        j = 0
        while j < p:
            arr = []
            start = j*offset
            end = (j*offset) + offset
            for i in range(len(M)):
                arr += M[i][start:end]
        
            yield self.sliptColumn(arr,offset)
            j += 1

    def sliptColumn(self,column, offset):
        slipt = [[] for i in range(offset)]
        for i in range(len(column)):
            slipt[i%offset].append(column[i])
        return slipt

        # Calculate the cell value of row and a column
    def calculate(self,row,column):
        ans = 0
        for count in range(len(row)):
            ans += (row[count]*column[count])
        return ans

# Mapper method
    def map(self, key, value):
        num_processes = int(self.opts.num_processes)
        N = int(self.opts.row_size)
        p = int(math.sqrt(num_processes))
        offset = int(N//p)
        # print(value)
        process_rank = 1
        if key == 'row':
            for i in self.rowMajor(value,p,offset):
                for z in range(p): #pass the root p
                    yield str(process_rank), i
                    process_rank+=1
            
        else:
            for j in self.columnMajor(value,p,offset):
                for x in range(0,num_processes,p): #pass the p, root p
                    yield str(x+process_rank) ,j
                process_rank +=1 

        
        # for i in self.rowMajor(value[0],p,offset):
        #     for j in self.columnMajor(value[1],p,offset):
        #         yield str(process_rank), i
        #         yield str(process_rank), j
        #         process_rank += 1
        
    #  The reducer method  
    def reduce(self, key, values):
        
        matrice = [ i for i in values]
        answer = []
        # print(matrice)
        for row in matrice[:1][0]:
            for column in matrice[1:][0]:
                answer.append(self.calculate(row,column))
        yield  answer
        
        

    def yieldAB(self,A,B):
        yield 'row',A
        yield 'column',B
        
    def convertMatrix(self,rank,N,offset):
        indice = []
        
        # start = rank - 1
        # i = (start // 2) * offset
        # j = (start % 2) * offset

        start = (rank-1)*offset
        i = ((start)//N)*offset
        j = (start)%N
        # return i,j
        for k in range(offset*offset):
            u = (k//offset)+i
            v = (k%offset)+j
            indice.append((int(u),v))
        return indice  


# Facilitate the data flow between the mapper and the reducer
    def run(self, job):
        num_processes = int(self.opts.num_processes)
        N = self.opts.row_size
        offset = int(N//math.sqrt(num_processes))
        matrixA = []
        matrixB = []
        matrixC = self.generateMatrix(N)
        for fileIndex in range(len(self.args[:-1])):
            with open(self.args[fileIndex],'r') as file:
                for line in file.readlines():
                    if(fileIndex == 0):
                        # print(line.strip().split(" "))
                        matrixA.append([int(index) for index in line.strip().split(" ")])
                    else:
                        # print(line)
                        matrixB.append([int(index) for index in line.strip().split(" ")])
            
        
        kvpairs = self.yieldAB(matrixA,matrixB)

        
        source = job.local_data(kvpairs)
        
        intermediate = job.map_data(source, self.map)
        source.close()
        output = job.reduce_data(intermediate, self.reduce)
        intermediate.close()

        job.wait(output)
        output.fetchall()


        for i in output.data():
            tempK = self.convertMatrix(int(i[0]),N,offset)
            # print(tempK)  
            for k,v in zip(tempK,i[1]):
                # print(k)
                matrixC[k[0]][k[1]] = v
        print(len(matrixA))
        # for i in matrixC:
        #     print(i)
            
        sys.stdout.flush()

        return 0
    @classmethod
    def update_parser(cls, parser):
        parser.add_option('-P', '--num_processes',
                        dest='num_processes',type='int',
                        help='Number of points for each map task',
                        default=2)

        parser.add_option('-N', '--row_size',
                        dest='row_size', type='int',
                        help='Number of map tasks to use',
                        default=16)
        return parser

if __name__ == '__main__':
    mrs.main(MatrixMultiplication)