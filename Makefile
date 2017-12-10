all: program

program: node.o 
	mpic++ node.o -o node

node.o: node.cpp 
	mpic++ -c node.cpp -o node.o

clean:
	rm -f node.o node