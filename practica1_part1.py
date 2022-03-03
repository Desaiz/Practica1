# -*- coding: utf-8 -*-
from multiprocessing import Process
from multiprocessing import Semaphore, Lock
from multiprocessing import current_process
from multiprocessing import Value, Array
from random import randint, random
from time import sleep

N = 200 #Vueltas
NPROD = 6 #nº productores

def delay(factor = 3):
    sleep(random()/factor)
    
def awake_producer(storage, mutex):
    #permitira a merge consumir mientras haya un elemento distinto de -1
    mutex.acquire()
    try:
        boolean = False
        for i in range(NPROD):
            boolean = boolean or (storage[i]!=-1)
        return boolean
    finally:
        mutex.release()

def add_data(storage, index, last, mutex):
    #last es un array que guarda el ultimo valor producido por el productor index
    mutex.acquire()
    try:
        product = last[index] + randint(0,5)
        storage[index] = product
    #se sobrescribe el ultimo valor con el nuevo para que se produzca de forma creciente
        last[index] = product
    finally:
        mutex.release()
    return product

def min_product(storage, mutex):
    mutex.acquire()
    try:
        #como minimo inicial escogemos el primer valor distinto de -1, llevamos una cuenta de la posicion
        i = 0
        while storage[i] == -1:
            i = i+1
        min_product = storage[i]
        j = i
        #escogemos el producto de menor valor
        for i in range(i, NPROD):
            if (storage[i] < min_product) and (storage[i] != -1):
                min_product = storage[i]
                j = i
        
    finally:
        mutex.release()
    return min_product, j

def get_data(storage, consumed_products, product, index, cont, mutex):
    mutex.acquire()
    try:
       
        consumed_products[cont.value]=(product)
        cont.value = cont.value + 1
    finally:
        mutex.release()
    
    

def finish(storage, index, mutex):
    #para acabar almacenamos un -1
    mutex.acquire()
    try:
        storage[index] = -1
    finally:
        mutex.release()
        
        
def producer(storage, empty_lst, non_empty_lst, last_values, mutex):
    for i in range (N):
        print (f"productor {current_process().name} produciendo")
        k = int(current_process().name.split('_')[1])
        empty_lst[k].acquire()
        product = add_data(storage, k, last_values, mutex)
        delay()
        non_empty_lst[k].release()
        print (f"productor {current_process().name} almacenado {product}")
        print(storage[:])
      
    k = int(current_process().name.split('_')[1])
    print (f"productor {k} acabando")
    empty_lst[k].acquire()
    finish(storage, k, mutex)
    non_empty_lst[k].release()  
    
        
        
def merge(storage, empty_lst, non_empty_lst, consumed_products, cont, mutex):
    for i in range(NPROD):
        non_empty_lst[i].acquire()
    while awake_producer(storage, mutex):
        product, from_producer = min_product(storage, mutex)
        get_data(storage, consumed_products, product, from_producer, cont, mutex)
        print (f"merge desalmacenando {product} de productor {from_producer}")
        empty_lst[from_producer].release()
        non_empty_lst[from_producer].acquire()
        
    
    
    

def main():
    storage = Array('i', NPROD)
    
    for i in range(NPROD):
        storage[i] = -1
    print ("almacen inicial", storage[:])
    last_values = Array('i', NPROD)
    for i in range(NPROD):
        last_values[i] = randint(0,5)
    empty_lst = [Lock() for i in range(NPROD)]
    non_empty_lst = [Semaphore(0) for i in range(NPROD)]
    consumed_products = Array('i',NPROD*N)
    cont = Value('i',0)
    mutex = Lock()
    prodlst = [ Process(target=producer,
                        name=f'prod_{i}',
                        args=(storage, empty_lst, non_empty_lst, last_values, mutex))
                for i in range(NPROD) ]

    consumidor = Process(target=merge, name= "consumidor", 
                         args=(storage, empty_lst, non_empty_lst, consumed_products, cont, mutex))
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
