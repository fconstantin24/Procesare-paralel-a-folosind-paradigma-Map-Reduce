# APD - Tema 1

build:
	@echo "Building..."
	@gcc -o tema1 tema1.c -lpthread
	@echo "Done"

clean:
	@echo "Cleaning..."
	@rm -rf tema1
	@echo "Done"