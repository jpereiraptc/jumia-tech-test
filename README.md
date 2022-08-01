# Data Engineer Exercise - MDS

## Retained Users per Month

Consider that we have a table that contains the login registry. This table has three columns: country, user_id, login_date

```
......................................................  
| country | user_id | login_date |  
|————-----|—-———----|—-——-——-----|  
| CI      | 420136  | 2020-05-15 |  
........................................................  
```

Write a query that gets the number of retained users per country and month. Here
retention for a given month is defined as the number of users who logged in that month who also logged in the immediately previous month.

## Distributed Word Counter

Write a python program that takes as input the path of a txt file and creates another file having the number of occurrences of each word in the original file in descending order.

Ex:
>the: 51021  
a: 12322  
to: 6543  
that: 2320  

The computation must be done in parallel using half the available machine cores.

## Most connected user

Download the following dataset:
https://snap.stanford.edu/data/higgs-social_network.edgelist.gz

It contains two columns, both with a social network's user ids in the following logic:  
user_id1 → user_id2  
Each line means a connection which can be interpreted as user_id 1 friend of user_id 2  
Using Spark (you can choose which language Java, Scala, Python) list the users by order of most connected to least