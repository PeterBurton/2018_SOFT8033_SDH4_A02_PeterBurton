# --------------------------------------------------------
#           PYTHON PROGRAM
# Here is where we are going to define our set of...
# - Imports
# - Global Variables
# - Functions
# ...to achieve the functionality required.
# When executing > python 'this_file'.py in a terminal,
# the Python interpreter will load our program,
# but it will execute nothing yet.
# --------------------------------------------------------

import json

# ------------------------------------------
# FUNCTION parse_function
# ------------------------------------------
def parse_function(x):
  cuisine = x["cuisine"]
  evaluation = x["evaluation"]
  points = x["points"]
  
  return(cuisine,(points,evaluation))

# ------------------------------------------
# FUNCTION get_totals
# ------------------------------------------
def get_totals(x):
  reviews = 0
  negreviews = 0
  points = 0
  for i in x[1]:
    reviews = reviews+1
    if i[1][1] == "Positive":
      points = points + i[1][0]
    else:
      negreviews = negreviews+1
      points = points - i[1][0]
  
  average = points/reviews
  
  return (x[0],(reviews, negreviews, points, average))

# ------------------------------------------
# FUNCTION filter_data
# ------------------------------------------
def filter_data(x,percentage_f, average):
  #if reviews <=average
  if x[1][0] <= average:
    return False
  #elif negreviews/reviews*100 > percentage_f
  elif x[1][1]/x[1][0]*100 > percentage_f:
    return False
  else:
    return True


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(dataset_dir, result_dir, percentage_f):
  
  #Read in data
  input_rdd = sc.textFile(dataset_dir)
  #Convert to Python dictionary with json.loads
  dict_rdd = input_rdd.map(lambda x: json.loads(x))
  #Persist dict_rdd for average calculation
  dict_rdd.persist()
  
  #Parse data to give following tuple with nested tuples: (cuisine,(points,evaluation)), and then group by cuisine
  grouped_rdd = dict_rdd.map(lambda x: parse_function(x)).groupBy(lambda x: x[0])
  #print(grouped_rdd.take(10))
  
  #Get total reviews, total neg reviews, total points and average
  #tuple returned is (cuisine,(reviews, negreviews, points, average))
  totals_rdd = grouped_rdd.map(lambda x: get_totals(x))
  #print(totals_rdd.take(10))
  #Persist totals_rdd for average calculation
  totals_rdd.persist()
  
  #Calculate the average
  average = dict_rdd.count()/totals_rdd.count()
  
  #Filter out entries that don't meet criteria in the spec and sort by average points per review descending
  filtered_rdd = totals_rdd.filter(lambda x: filter_data(x,percentage_f, average)).sortBy(lambda x: -x[1][3])
  #print(filtered_rdd.take(10))
  
  filtered_rdd.saveAsTextFile(result_dir)
  

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We provide the path to the input folder (dataset) and output folder (Spark job result)
    source_dir = "/FileStore/tables/A02/my_dataset/"
    result_dir = "/FileStore/tables/A02/my_result/"

    # 2. We add any extra variable we want to use
    percentage_f = 10

    # 3. We remove the monitoring and output directories
    dbutils.fs.rm(result_dir, True)

    # 4. We call to our main function
    my_main(source_dir, result_dir, percentage_f)
