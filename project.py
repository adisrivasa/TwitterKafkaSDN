from producer import *
from localConsumer import *
from analysisConsumer import *
from elasticConsumer import *

def mainMenu():
    consumer_response = 0
    NofT = 0
    print("Welcome to Twitter Kafka streaming service")
    # print("Please enter the number of terms you would like to search for:")
    stream_name = str(input("Please enter the stream_name: "))
    print("Topic is created")
    NofT = int(input("Please enter the number of terms you would like to search for: "))
    print("Enter the terms, one at a time\n")
    lst = []
    for i in range(0, NofT):
        elem = str(input())

        lst.append(elem)

    runprog(lst)
    print(1)
    print("Choose the type of consumer you want to run:")
    while consumer_response != -1:
        consumer_response = get_menu('\n\tMenu\n\t1. Local(Update the csv)\n\t2. Storage(Update Elasticsearch DB)\n\t3. Remote(Flask app)\n\t4. Quit', ['local', 'store', 'remote', 'quit'])
        if consumer_response == 1:
            local()
        elif consumer_response == 2:
            store()
        elif consumer_response == 3:
            remote()
        elif consumer_response == 4:
            print("Exiting program")
            break
        else:
            print("Incorrect! Please enter again")
            continue

def get_menu(prompt, choices):
    print (prompt)
    count = len(choices)
    response = 0
    while response < 1 or response > count:
        response = input('Type a number (1-{}): '.format(count))
        if response.isdigit():
            response = int(response)
        else:
            response = 0
    return response

if __name__ == '__main__':
    mainMenu()
