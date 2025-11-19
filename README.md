# Spark

## Aim:
Write a PySpark program to perform data analytics by loading a large dataset into Spark, cleaning it, and computing statistical summaries and aggregations.

## Algorithm:

## Student Marks Analysis

### Step 1: Start SparkSession

Create a Spark session if not already running.

### Step 2: Load the CSV dataset

Use spark.read with header and schema inference to load students.csv.

### Step 3: Identify Pass and Fail students

Apply filter Marks ≥ 50 → Pass

Apply filter Marks < 50 → Fail

### Step 4: Compute subject-wise average marks

Group by Subject and apply avg(Marks).

### Step 5: Find topper per subject

Create a window partitioned by Subject, ordered by Marks DESC

Use row_number() to rank students

Select rows where rank = 1

### Step 6: Display all outputs

Show pass list, fail list, averages, and toppers.

## Dataset:

### Name,Subject,Marks
### Dhana,Math,78
### Ravi,Math,46
### Meena,Physics,82
### Ravi,Physics,66
### Anu,Math,92
### Meena,Math,48

## Run this once if you are NOT in pyspark REPL (e.g., Colab / Jupyter)
```Python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Unit2Labs").getOrCreate()
from pyspark.sql import functions as F
from pyspark.sql.window import Window
```

## load
```Python
df = spark.read.option("header",True).option("inferSchema",True).csv("data/students.csv")
```

## 1. Pass (>=50) and Fail (<50)
```Python
pass_df = df.filter(F.col("Marks") >= 50)
fail_df = df.filter(F.col("Marks") < 50)
```
## show
```Python
print("Pass students:")
pass_df.show()
print("Fail students:")
fail_df.show()
```

## 2. Subject-wise average marks
```Python
avg_by_subject = df.groupBy("Subject").agg(F.avg("Marks").alias("avg_marks"))
avg_by_subject.show()
```


## 3. Topper (max marks) per subject - show Name and Marks
```Python
w = Window.partitionBy("Subject").orderBy(F.desc("Marks"))
topper = df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).select("Subject","Name","Marks")
topper.show()
```


## Bank Transactions Analysis

### Step 1: Start SparkSession

Ensure Spark is running.

### Step 2: Load the transactions dataset

Read transactions.csv with header and schema inference.

### Step 3: Calculate total debit and credit per customer

Group by CustID and Type

Aggregate using sum(Amount)

Pivot on Type (debit/credit) for cleaner output

### Step 4: Identify highest single transaction

Sort rows by Amount DESC and take the first record.

### Step 5: Detect suspicious transactions

Filter rows where Amount > 50000.

### Step 6: Display results

Show totals, highest transaction, and suspicious entries.


### Dataset 
### CustID,Amount,Type
### C001,25000,credit
### C002,15000,debit
### C001,60000,debit
### C003,78000,credit
### C002,51000,credit
### C004,45000,debit

```Python
tx = spark.read.option("header",True).option("inferSchema",True).csv("data/transactions.csv")
```

### 1. Total debit and credit per customer

```Python

totals = tx.groupBy("CustID", "Type").agg(F.sum("Amount").alias("total_amount"))
### Optionally pivot to have debit/credit in columns
totals_pivot = tx.groupBy("CustID").pivot("Type", ["debit","credit"]).sum("Amount").na.fill(0)
totals_pivot.show()
```

### 2. Customer with highest single transaction
```Python
highest_tx = tx.orderBy(F.desc("Amount")).limit(1)
highest_tx.show()
```

### 3. Suspicious transactions (Amount > 50000)
```Python
suspicious = tx.filter(F.col("Amount") > 50000)
suspicious.show()
```

## Employee Attendance Analysis

### Step 1: Start SparkSession

Initialize Spark session.

### Step 2: Load the attendance dataset

Read attendance.csv with header and inferSchema.

### Step 3: Compute attendance summary for each employee

For each EmpID:

Count total days

Count present days using when(Status=='P',1)

Count absent days using when(Status=='A',1)

Calculate attendance % = present_days / total_days × 100

### Step 4: Identify employees with low attendance

Filter employees where attendance% < 75.

### Step 5: Identify most regular employee

Compute maximum attendance percentage

Select employee(s) whose attendance% equals this max value

### Step 6: Display outputs

Show attendance table, low-attendance employees, and most regular employee.


```Python
att = spark.read.option("header",True).option("inferSchema",True).csv("data/attendance.csv")
```

## Total present and absent days per employee
```Python
agg_att = att.groupBy("EmpID").agg(
    F.count("*").alias("total_days"),
    F.sum(F.when(F.col("Status") == "P", 1).otherwise(0)).alias("present_days"),
    F.sum(F.when(F.col("Status") == "A", 1).otherwise(0)).alias("absent_days")
).withColumn("attendance_pct", F.round(100 * F.col("present_days") / F.col("total_days"), 2))

agg_att.show()
```

## Employees with attendance < 75%
```Python
low_att = agg_att.filter(F.col("attendance_pct") < 75)
low_att.show()
```

## Most regular employee(s) - highest attendance_pct
```Python
max_pct = agg_att.agg(F.max("attendance_pct").alias("max_pct")).collect()[0]["max_pct"]
most_regular = agg_att.filter(F.col("attendance_pct") == max_pct)
most_regular.show()

att = spark.read.option("header",True).option("inferSchema",True).csv("data/attendance.csv")
```

## Total present and absent days per employee
```Python
agg_att = att.groupBy("EmpID").agg(
    F.count("*").alias("total_days"),
    F.sum(F.when(F.col("Status") == "P", 1).otherwise(0)).alias("present_days"),
    F.sum(F.when(F.col("Status") == "A", 1).otherwise(0)).alias("absent_days")
).withColumn("attendance_pct", F.round(100 * F.col("present_days") / F.col("total_days"), 2))

agg_att.show()
```
## Employees with attendance < 75%
```Python
low_att = agg_att.filter(F.col("attendance_pct") < 75)
low_att.show()
```
## Most regular employee(s) - highest attendance_pct
```Python
max_pct = agg_att.agg(F.max("attendance_pct").alias("max_pct")).collect()[0]["max_pct"]
most_regular = agg_att.filter(F.col("attendance_pct") == max_pct)
most_regular.show()
```

## Number Analysis with Dataset

### Step 1: Start SparkSession

Initialize Spark.

### Step 2: Load the dataset

Read numbers.csv with header and schema inference.

### Step 3: Identify even and odd numbers

Apply modulo operation % 2

Filter values where remainder = 0 → even

Remainder ≠ 0 → odd

### Step 4: Compute statistical summary

Use agg() to calculate:

Maximum value

Minimum value

Sum of values

Average value (rounded to 2 decimals)

### Step 5: Define a UDF to test primality

Implement is_prime(n) returning True/False based on prime number logic.

### Step 6: Register UDF and filter prime numbers

Apply UDF on Value column to get list of primes.

### Step 7: Perform RDD-based computations

Convert DataFrame to RDD and compute:

Total sum (using reduce)

Count of even numbers (using filter + count)

### Step 8: Display all results

Show even numbers, odd numbers, stats, primes, and RDD outputs.


### Dataset data/numbers.csv

#### Value
#### 2
#### 3
#### 4
#### 17
#### 20
#### 121

```Python
nums = spark.read.option("header",True).option("inferSchema",True).csv("data/numbers.csv")
```
# even and odd
```Python
even = nums.filter((F.col("Value") % 2) == 0)
odd  = nums.filter((F.col("Value") % 2) != 0)
print("Evens:"); even.show()
print("Odds:");  odd.show()
```
# stats: max, min, sum, avg
```Python
stats = nums.agg(
    F.max("Value").alias("max_val"),
    F.min("Value").alias("min_val"),
    F.sum("Value").alias("sum_val"),
    F.round(F.avg("Value"),2).alias("avg_val")
)
stats.show()
```
# prime number check 
```Python
from pyspark.sql.types import BooleanType
def is_prime(n):
    if n is None or n < 2:
        return False
    if n == 2:
        return True
    if n % 2 == 0:
        return False
    i = 3
    while i * i <= n:
        if n % i == 0:
            return False
        i += 2
    return True

is_prime_udf = F.udf(is_prime, BooleanType())
primes = nums.filter(is_prime_udf(F.col("Value")))
print("Primes:")
primes.show()


rdd = nums.rdd.map(lambda row: row["Value"])
total_sum = rdd.reduce(lambda a,b: a+b)
even_count = rdd.filter(lambda x: x % 2 == 0).count()
print("sum=", total_sum, "even_count=", even_count)

```

## Logical Analysis on Age Data

### Step 1: Start SparkSession

Ensure Spark is active.

### Step 2: Load the dataset

Read people.csv with header and inferSchema options.

### Step 3: Categorize people by age

Use when() conditions to classify into:

Minor (Age < 18)

Adult (18–59)

Senior (Age ≥ 60)

### Step 4: Count number of people in each category

Group by Category and apply count().

### Step 5: Find oldest and youngest person

Order by Age descending → oldest

Order by Age ascending → youngest

Pick top record in each case

### Step 6: Display results

Show categorized table, category counts, oldest and youngest persons.


### Dataset 
### Name,Age
### Dhana,29
### Ravi,16
### Anu,64
### Meena,45
### Kumar,12

```Python
people = spark.read.option("header",True).option("inferSchema",True).csv("data/people.csv")
```

### 1. Categorize
```Python
people_cat = people.withColumn(
    "Category",
    F.when(F.col("Age") < 18, "Minor")
     .when((F.col("Age") >= 18) & (F.col("Age") <= 59), "Adult")
     .otherwise("Senior")
)
people_cat.show()
```

### 2. Count per category
```Python
people_cat.groupBy("Category").count().show()
```

### 3. Oldest and youngest person
```Python
oldest = people.orderBy(F.desc("Age")).limit(1)
youngest = people.orderBy(F.asc("Age")).limit(1)
print("Oldest:"); oldest.show()
print("Youngest:"); youngest.show()
```


## Product Sales Analysis
## Step 1: Start SparkSession

Initialize Spark.

## Step 2: Load the dataset

Read sales.csv with header and inferSchema.

## Step 3: Compute revenue per product

Create a new column:
Revenue = Quantity × Price

## Step 4: Aggregate sales per product

Group by Product & Category and compute:

Total revenue

Total quantity sold

## Step 5: Identify best-selling product

Sort aggregated quantity in descending order and select highest.

## Step 6: Identify best-selling category

Group by Category and compute sum of Quantity; pick the largest.

## Step 7: Identify low-sales products

Filter products where total quantity < 100 units.

## Step 8: Display all outputs

Show revenue table, best product, best category, and low-sales products.


### Dataset 

### Product,Category,Quantity,Price
### Soap,PersonalCare,120,20.5
### Shampoo,PersonalCare,80,120.0
### Rice,Groceries,500,40.0
### Sugar,Groceries,60,45.0
### Notebook,Stationery,30,25.0

```Python
sales = spark.read.option("header",True).option("inferSchema",True).csv("data/sales.csv")
```

# 1. Revenue per product
```Python
sales_with_rev = sales.withColumn("Revenue", F.col("Quantity") * F.col("Price"))
rev_per_product = sales_with_rev.groupBy("Product","Category").agg(F.sum("Revenue").alias("total_revenue"), F.sum("Quantity").alias("total_qty"))
rev_per_product.orderBy(F.desc("total_revenue")).show()
```

# 2. Best-selling product and category (by quantity)
```Python
best_product = rev_per_product.orderBy(F.desc("total_qty")).limit(1)
best_product.show()
best_category = sales.groupBy("Category").agg(F.sum("Quantity").alias("category_qty")).orderBy(F.desc("category_qty")).limit(1)
best_category.show()
```

# 3. Products with sales below 100 units (total quantity across dataset)
```Python
low_sales_products = rev_per_product.filter(F.col("total_qty") < 100)
low_sales_products.show()
```







