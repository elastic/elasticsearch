---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-statements.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Statements [painless-statements]

Statements are the smallest individual units that are compiled in your Painless scripts. They control the flow of the code, define logic branches, and manage how your code processes data. Painless supports all Java [control flow statements](https://dev.java/learn/language-basics/controlling-flow/) except the `switch` statement.

Statements in Painless allow you to create conditional logic, iterate through data, and structure your scripts for complex data processing tasks. Understanding these control structures is essential for writing effective scripts that can handle various scenarios in {{es}} workflows.

## Conditional statements [_conditional_statements]

Conditional statements enable your script to run different code paths based on values in the data.

### If / Else [_if_else]

Use `if`, `else if`, and `else` statements to create conditional logic that runs different code blocks based on boolean expressions.

#### Example

In this example, the product’s category is determined based on the product’s price value, classifying it as "Affordable"m "Moderately priced", or "Expensive".

```java
int price = 64;
String priceCategory;

if (price > 60) {
    priceCategory = "Expensive";
} else if (price < 30) {
    priceCategory = "Affordable";
} else {
    priceCategory = "Moderately Priced";
}

return priceCategory; // Expensive
```

### Ternary operator

The ternary operator (`? :`) provides a concise way to perform conditional assignments. It’s a conditional statement that achieves the same purpose as `if/else` but in a more compact form.

#### Example

In this example, the product’s category is determined based on the product’s price value, classifying it as "Affordable" or "Expensive".

```java
int price = 64;
String priceCategory;

priceCategory = (price >= 60) ? "Affordable" : "Expensive";

return priceCategory; // Expensive
```

## Loop statements [_loop_statements]

Loop statements allow you to repeat running code multiple times, either for a specific number of iterations or while a condition remains true. 

### For

Painless supports both traditional `for` loops and enhanced `for` loops (`for-each`). Use `for` loops to iterate through data collections or repeat operations a specific number of times.

#### Examples

* Traditional for loop:

    The following loop creates an empty array with four positions and assigns a value from `0` to `3` to each position.

    ```java
    int[] arr = new int[4];

    for (int i = 0; i < 4; i++) {
    arr[i] = i;
    }

    return arr; // [0, 1, 2, 3] 
    ```

* Enhanced `for` loop (for-each):

    The following code snippets create a list containing letters. Using a `for-each` loop, they concatenate the letters into a single string called `word`.

    ```java
    List letters = ["h", "e", "l", "l", "o"];
    String word = "";

    for (l in letters) {
      word += l;
    }

    return word; // hello
    ```

* Alternative `for` loop syntax:

    ```java
    List letters = ["h", "e", "l", "l", "o"];
    String word = "";

    for (def l : letters) {
      word += l;
    }

    return word; // hello
    ```

### While

Use `while` loops to repeat running code as long as a specified condition remains true. The condition is evaluated before each iteration.

#### Example

Similar to the first example for the `for` statement, this one assigns a number from `0` to `3` to each position of an array with four elements using a `while` loop.

```java
int[] arr = new int[4];
int i = 0; // counter

while (i < 4) {
    arr[i] = i;
    i++; // increment counter
}

return arr; // [0, 1, 2, 3]
```

### Do-While

Use `do-while` loops to run code at least once, then repeat as long as the condition remains true. The condition is evaluated after each iteration.

#### Example

This code defines an array with mixed types (strings and integers) and uses a `do-while` loop to concatenate all elements into a single string called word.

```java
def[] letters = new def[] {"a", 1, "b", 2};
String word = "";
int i = 0; // counte
do {
    word += letters[i];
    i++; // increment counter
} while (i < letters.length)
return word; // a1b2
```
