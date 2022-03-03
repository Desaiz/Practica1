# -*- coding: utf-8 -*-
from multiprocessing import Process
from multiprocessing import Semaphore, Lock
from multiprocessing import current_process
from multiprocessing import Value, Array
from random import randint, random
from time import sleep


N = 30#Vueltas
K = 10 #Buffer de los consumidores
NPROD = 5 #nº productores
TOTAL = K*NPROD

def delay(factor = 3):
    sleep(random()/factor)

def awake_producer(storage, mutex):
    mutex.acquire()
    try:
        boolean = False
        for i in range(TOTAL):
            boolean = boolean or (storage[i]!=-1)
        return boolean
    finally:
        mutex.release()
        
def add_data(storage, ind, last_value, last_index, mutex):
    mutex.acquire()
    try:
        r = last_index[ind]
        product = last_value[ind] + randint(0,5)
        storage[ind*K+r] = product
        last_value[ind] = product
        last_index[ind] = last_index[ind] + 1
    finally:
        mutex.release()
    return product

def min_product(storage, mutex):
    mutex.acquire()
    try:
        i = 0#Buscamos el primer elemento distinto del -1. 
        while storage[i] == -1:
            i = i+K#restringimos la búsqueda solo al primer elemento del buffer de cada productor
        min_product = storage[i]
        j = i//K
        #recorremos lo que queda de array buscando un elemento menor
        for s in range(i, TOTAL, K):
            if ((storage[s] < min_product) and (storage[s] != -1)):
                min_product, j = storage[s], s//K
    finally:
        mutex.release()
    return min_product, j

def get_data(storage, consumed_products, product, index, last_index, cont, mutex):
    mutex.acquire()
    try:
        consumed_products[cont.value] = product
        cont.value = cont.value + 1
        for i in range(index*K, (index+1)*K-1):
            storage[i] = storage[i + 1]
        storage[(index+1)*K-1] = -1
        last_index[index] = last_index[index] - 1
    finally:
        mutex.release()
    

def finish(storage, index, mutex):
    mutex.acquire()
    try:
        storage[(index+1)*K-1] = -1
        
    finally:
        mutex.release()
        
        
def producer(storage, empty_lst, non_empty_lst, last_values, last_index, mutex):
    for i in range (N):
        print (f"productor {current_process().name} produciendo")
        k = int(current_process().name.split('_')[1])
        empty_lst[k].acquire()
        product = add_data(storage, k, last_values, last_index, mutex)
        delay()
        non_empty_lst[k].release()
        print (f"productor {current_process().name} almacenado {product}")
        print(storage[:])
    
    k = int(current_process().name.split('_')[1])
    empty_lst[k].acquire()
    finish(storage, k, mutex)
    non_empty_lst[k].release()  
    
        
        
def merge(storage, empty_lst, non_empty_lst, consumed_products, last_index, cont, mutex):
    for i in range(NPROD):
        non_empty_lst[i].acquire()
    while awake_producer(storage, mutex):
        product, from_producer = min_product(storage, mutex)
        get_data(storage, consumed_products, product, from_producer, last_index, cont, mutex)
        print (f"merge desalmacenando {product} de productor {from_producer}")
        print (storage[:])
        empty_lst[from_producer].release()
        non_empty_lst[from_producer].acquire()
        

def main():
    storage = Array('i', TOTAL)
    for i in range(TOTAL):
        storage[i] = -1
    print ("almacen inicial", storage[:])
    
    last_values = Array('i', NPROD)
    for i in range(NPROD):
        last_values[i] = randint(0,5)
        
    last_index = Array('i', NPROD)
    for i in range(NPROD):
        last_index[i] = 0
    
    consumed_products = Array('i', NPROD*N)
    cont = Value('i',0)
    
    empty_lst = [Semaphore(K) for i in range(NPROD)]
    non_empty_lst = [Semaphore(0) for i in range(NPROD)]
    mutex = Lock()
     
    prodlst = [ Process(target=producer,
                        name=f'prod_{i}',
                        args=(storage, empty_lst, non_empty_lst, last_values, last_index, mutex))
                for i in range(NPROD) ]

    consumidor = Process(target=merge, name= "consumidor", 
                         args=(storage, empty_lst, non_empty_lst, consumed_products, last_index, cont, mutex))
    procs = [consumidor] + prodlst
    for p in procs:
        p.start()
    
    for p in procs:
        p.join()
    
    print("--------------------------------------------")
    print("Almacén final", storage[:])
    print("Lista final de consumiciones:", consumed_products[:])

if __name__ == '__main__':
    main()
