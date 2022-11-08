#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <pthread.h>
#define LONG_MAX 2147483647

pthread_barrier_t barrier1;
pthread_barrier_t barrier2;

// structure for mapper arguments
typedef struct {
    long id;
    int M, R, nr, nr_files;
    char** mtx;
    int *file_indexes;
} Mapper_args;

// structure for reducer arguments
typedef struct {
    long id;
    int M, R;
} Reducer_args;

// function for mapper operations
void *f_mapper(void *arg) {
  	Mapper_args m_a = *(Mapper_args*)arg;

    printf("Mapper %ld %d %d \n", m_a.id, m_a.M, m_a.R);


    //open test files and read numbers
    FILE *f;
    for (int i = 0; i < m_a.nr_files; i++) {
        f = fopen(m_a.mtx[m_a.file_indexes[i]], "r");
        if (!f) {
            printf("Cannot open the file!");
            return 0;
        }
    }
    pthread_barrier_wait(&barrier1);
  	pthread_exit(NULL);
}

// function for reducer operations
void *f_reducer(void *arg) {
    pthread_barrier_wait(&barrier2);
  	Reducer_args r_a = *(Reducer_args*)arg;
  	printf("Reducer %ld\n", r_a.id);
  	pthread_exit(NULL);
}

/* argc = no. of arguments
   argv = array of pointers listing all the arguments */
int main(int argc, char *argv[]) {  

    //reading the arguments
    if (argc < 4) {
        printf("Run the program with 4 parameters!");
        return 0;
    }
    // number of mapper and reducer threads
    int M, R;
    sscanf(argv[1], "%d", &M);
    sscanf(argv[2], "%d", &R);
    char* F_in = malloc(20);
    strcpy(F_in, argv[3]);

    //open file containing the name of the test files
    FILE* f;
    f = fopen(F_in, "r");
    if (!f) {
        printf("Cannot open the file!");
        return 0;
    }

    //read number of files
    int nr;
    fscanf(f,"%d", &nr);
    
    //create matrix to save file names
    int col = 20;
    char** mtx = (char**)malloc(nr * sizeof(char*));
    for (int i = 0; i < nr; i++) {
        mtx[i] = (char*)malloc(col * sizeof(char));
        fscanf(f, "%s", mtx[i]);
    }
    fclose(f);

    //open test files and save file dimensions
    long int array[nr];
    long int total_memory = 0;
    for (int i =0; i< nr; i++) {
        f = fopen(mtx[i], "r");
        fseek(f, 0L, SEEK_END);
        array[i] = ftell(f);
        total_memory += array[i];
        fclose(f);
    }

    //calculate efficient scalability for the files
    // memories will save the ids of the files in each thread
    // indexes will save how many ids each thread will contain
    int memories[M][nr];
    long int *memory = calloc(M, sizeof(long int));
    int *indexes = calloc(M, sizeof(int));
    int min_index = 0;
    
    for (int i = 0; i < nr; i++) {
        memory[min_index] += array[i];
        memories[min_index][indexes[min_index]] = i;
        indexes[min_index]++;

        long int minim = LONG_MAX;
        for (int j = 0; j < M; j++) {
            if(minim > memory[j]) {
                minim = memory[j];
                min_index = j;
            }
        }
    }

    //open threads
    pthread_t threads[M + R];
    int r;
  	long id;
  	void *status;
    Mapper_args m_a[M];
    Reducer_args r_a[R];

    if (pthread_barrier_init(&barrier1, NULL, M) != 0) {
        printf("Barrier init has failed!\n");
        return 1;
    }
    if (pthread_barrier_init(&barrier2, NULL, M) != 0) {
        printf("Barrier init has failed!\n");
        return 1;
    }

    for (id = 0; id < M + R; id++) {
		if (id < M) {
            m_a[id].M = M;
            m_a[id].R = R;
            m_a[id].id = id;
            m_a[id].nr = nr;
            m_a[id].mtx = mtx;
            m_a[id].file_indexes = memories[id];
            m_a[id].nr_files = indexes[id];
            r = pthread_create(&threads[id], NULL, f_mapper, &m_a[id]);
        } else {
            r_a[id - M].id = id;
            r_a[id -M].M = M;
            r_a[id - M].R = R;
            r = pthread_create(&threads[id], NULL, f_reducer, &r_a[id - M]);
        }

		if (r) {
	  		printf("Eroare la crearea thread-ului %ld\n", id);
	  		exit(-1);
		}
  	}

    // close threads
    for (id = 0; id < M + R; id++) {
		r = pthread_join(threads[id], &status);

		if (r) {
	  		printf("Eroare la asteptarea thread-ului %ld\n", id);
	  		exit(-1);
		}
  	}
  	pthread_exit(NULL);
    pthread_barrier_destroy(&barrier1);
    pthread_barrier_destroy(&barrier2);

    //free alocated memory
    free(memory);
    free(indexes);
    free(F_in);
    for (int i = 0; i < nr; i++)
        free(mtx[i]);
    free(mtx);
    return 0;
}