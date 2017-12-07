all: program

program: node.o 
	mpicc node.o -o node

node.o: node.c list.h
	mpicc -c node.c -o node.o

clean:
	rm -f node.o