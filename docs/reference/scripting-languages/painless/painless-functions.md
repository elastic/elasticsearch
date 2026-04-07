---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-functions.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Functions [painless-functions]

A function is a reusable block of code that performs a specific task and can be called multiple times throughout your script. Functions help organize your code, reduce repetition, and make complex scripts more maintainable and readable.

Functions in Painless work similarly to Java functions, allowing you to encapsulate logic, accept input parameters, and return calculated results. You can define custom functions to handle common operations, calculations, or data transformations that your script needs to perform repeatedly.

## Function structure

In the statement(s) of a function, a parameter is a named type value available as a [variable](/reference/scripting-languages/painless/painless-variables.md). Each function specifies zero-to-many parameters, and when a function is called, a value is specified per parameter. An argument is a value passed into a function at the point of call. A function specifies a type value, though if the type is [void](/reference/scripting-languages/painless/painless-types.md#void-type) then no value is returned. Any non-void type return value is available for use within an [operation](/reference/scripting-languages/painless/painless-operators.md) or is discarded otherwise.

## Function declaration

You can declare functions at the beginning of a Painless script. Functions must be declared before they are used in your script.

### Examples

* Function with return value:

    This code calculates the VAT tax on an `amount = 100`

    ```java
    double calculateVAT(double amount) {
        return amount * 0.19; // return double type
    }

    return calculateVAT(100);
    ```

* Function with void return type:

    This snippet uses a function called `addName` to add the name “Elyssa” to a list of names.
    
    ```java
    void addName(List l, String n) {
        l.add(n);
    }
    
    List names = new ArrayList();
    addName(names, "Elyssa");
    ```

* Simple boolean function:

    This code uses a boolean function to evaluate the `if` statement. If the customer is prime, the code inside the     statement is run; otherwise, it continues with the next part of the program.
    
    ```java
    boolean hasFreeShipping(def customer) { customer.isPrime }
    ...
    if (hasFreeShipping(client)) {
     ...
    }
    ```

## Best practices

When you write functions in Painless:

* Use descriptive names that clearly indicate what the function does.  
* Define appropriate parameter types to ensure type safety.  
* Keep functions focused on a single, well-defined task.  
* Return consistent types to make functions predictable.  
* Declare functions at the beginning of your script for clarity.
