
import json
import os
#print(os.getcwd())

filename = input("Enter the file name: ")
user = input("Enter the user name: ")
ticket = input("Enter the ticket id: ")
start_date = input("Enter the start date: ")
end_date = input("Enter the end date: ")
try:
    
    market_list=[]
    market_test=int(input("Enter the market number: "))
    markets=market_test.split(',')
    for num in markets:
        market_list.append(num)
except ValueError:
    pass

test_list=[]
input_list=input("enter the exp names: ")
exports = input_list.split(",")
#test_list=exports.replace(" "," ").split(",")
for names in exports:
        test_list.append(names)


suffix = '.txt'
filename=os.path.join(filename + suffix)



with open(filename, 'w') as f:
    json.dump({
        "user": user,
        "ticket": ticket,
        "start_date": start_date,
        "end_date" : end_date,
        "market_nos" : market_list,
        "export_names" : test_list
        
}, f, indent=4)


print("JSON saved to file {}".format(os.path.abspath(filename)))