---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-debugging.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Painless debugging [painless-debugging]

## Why debugging in Painless is different

Painless scripts run within specific {{es}} contexts, not as isolated code. Unlike languages with interactive environments, Painless doesn’t provide a [REPL](https://en.wikipedia.org/wiki/Read%E2%80%93eval%E2%80%93print_loop) because script behavior depends entirely on the execution context and available data structures.

The context determines available parameters, API restrictions, and expected return types. A debugging approach that works in one context might not be directly applied to another because each context provides different capabilities and data access patterns. Refer to [Painless contexts](https://www.elastic.co/docs/reference/scripting-languages/painless/painless-contexts) to understand what variables and methods are available in each context.

## Context matters for debugging

The secure, sandboxed approach used in Painless prevents access to standard Java debugging information. The sandbox restricts scripts from accessing information like object types, which has the effect of preventing traditional Java debugging methods.

Scripts cannot access:

* Object type information through standard Java methods (like [`get_Class()`](https://docs.oracle.com/javase/8/docs/api/java/lang/Object.html#getClass--) or [reflection](https://docs.oracle.com/javase/8/docs/technotes/guides/reflection/index.html))  
* Stack traces beyond Painless script boundaries  
* Runtime class details for security-sensitive operations

## Debug methods available

Painless scripts do not provide interactive debugging tools or a REPL. The only official method for inspecting objects and their types during script execution is by means of the`Debug.explain(object)`. This method throws an informative exception that reveals the object's type and value. As a result, your script does not complete normally; instead, you an error message is included in the response.

You can use custom exceptions (for example, `throw new RuntimeException(...)`) to signal specific conditions or control execution flow but do not provide detailed object inspection. For comprehensive debugging, always use `Debug.explain()`.

## Debugging walkthrough

This section demonstrates a common debugging scenario using a script that formats the total price as "TOTAL: X" in uppercase. The example shows how to identify an error, debug it using `Debug.explain()`, and apply the fix.

### Step 1: Run the failing script

The following script attempts to create a formatted price string from the [ecommerce dataset](/reference/scripting-languages/painless/painless-context-examples.md). The script appears logical but fails:

```json
GET /kibana_sample_data_ecommerce/_search
{
  "size": 1,
  "query": {
    "match_all": {}
  },
  "script_fields": {
    "formatted_total": {
      "script": {
        "source": """
          // This will fail - trying to call toUpperCase() on a number
          return "TOTAL: " + doc['taxful_total_price'].toUpperCase();
        """
      }
    }
  }
}
```

### Step 2: Analyze the error

The script fails with this error message:

```json
{
  "took": 8,
  "timed_out": false,
  "_shards": {
    "total": 3,
    "successful": 2,
    "skipped": 0,
    "failed": 1,
    "failures": [
      {
        "shard": 0,
        "index": "kibana_sample_data_ecommerce",
        "node": "Rcc7GqOuTta0MV28FZXE_A",
        "reason": {
          "type": "script_exception",
          "reason": "runtime error",
          "script_stack": [
            """return "TOTAL: " + doc['taxful_total_price'].toUpperCase();
            """,
            "                                             ^---- HERE"
          ],
          "script": " ...",
          "lang": "painless",
          "position": {
            "offset": 62,
            "start": 14,
            "end": 90
          },
          "caused_by": {
            "type": "illegal_argument_exception",
            "reason": "dynamic method [java.lang.Double, toUpperCase/0] not found"
          }
        }
      }
    ]
  },
  "hits": {
    "total": {
      "value": 4675,
      "relation": "eq"
    },
    "max_score": 1,
    "hits": []
  }
}
```

The error indicates that the script attempts to call `toUpperCase()` on a `Double` object, but this method does not exist for numeric types. While the error message provides information about the problem; use `Debug.explain()` to gain additional clarity about the data type.

### Step 3: Use Debug.explain() to understand the problem

Use `Debug.explain()` to inspect the data type:

```json
GET /kibana_sample_data_ecommerce/_search
{
  "size": 1,
  "query": {
    "match_all": {}
  },
  "script_fields": {
    "debug_price_type": {
      "script": {
        "source": """
          Debug.explain(doc['taxful_total_price']);
          return "Check the error for debugging information.";
        """
      }
    }
  }
}
```

The debugging output reveals the data structure:

```json
{
  "took": 9,
  "timed_out": false,
  "_shards": {
    "total": 3,
    "successful": 2,
    "skipped": 0,
    "failed": 1,
    "failures": [
      {
        "shard": 0,
        "index": "kibana_sample_data_ecommerce",
        "node": "Rcc7GqOuTta0MV28FZXE_A",
        "reason": {
          "type": "script_exception",
          "reason": "runtime error",
          "painless_class": "org.elasticsearch.index.fielddata.ScriptDocValues$Doubles",
          "to_string": "[46.96875]",
          "java_class": "org.elasticsearch.index.fielddata.ScriptDocValues$Doubles",
          "script_stack": [
            """Debug.explain(doc['taxful_total_price']);
          """,
            "                                       ^---- HERE"
          ],
          "script": " ...",
          "lang": "painless",
          "position": {
            "offset": 46,
            "start": 11,
            "end": 62
          },
          "caused_by": {
            "type": "painless_explain_error",
            "reason": null
          }
        }
      }
    ]
  },
  "hits": {
    "total": {
      "value": 4675,
      "relation": "eq"
    },
    "max_score": 1,
    "hits": []
  }
}
```

The output shows that `doc['taxful_total_price']` is an `org.elasticsearch.index.fielddata.ScriptDocValues$Doubles` object with the value `[46.96875]`. This is an {{es}} class for handling numeric field values.   
To access the actual double value, use `doc['taxful_total_price'].value`.

Check the actual value:

```json
GET /kibana_sample_data_ecommerce/_search
{
  "size": 1,
  "query": {
    "match_all": {}
  },
  "script_fields": {
    "debug_price_value": {
      "script": {
        "source": """
          Debug.explain(doc['taxful_total_price'].value);
          return "Check the error for debugging information.";
        """
      }
    }
  }
}
```

This produces the following output:

```json
{
  "took": 5,
  "timed_out": false,
  "_shards": {
    "total": 3,
    "successful": 2,
    "skipped": 0,
    "failed": 1,
    "failures": [
      {
        "shard": 0,
        "index": "kibana_sample_data_ecommerce",
        "node": "Rcc7GqOuTta0MV28FZXE_A",
        "reason": {
          "type": "script_exception",
          "reason": "runtime error",
          "painless_class": "java.lang.Double",
          "to_string": "46.96875",
          "java_class": "java.lang.Double",
          "script_stack": [
            """Debug.explain(doc['taxful_total_price'].value);
          """,
            "                                             ^---- HERE"
          ],
          "script": " ...",
          "lang": "painless",
          "position": {
            "offset": 46,
            "start": 11,
            "end": 62
          },
          "caused_by": {
            "type": "painless_explain_error",
            "reason": null
          }
        }
      }
    ]
  },
  "hits": {
    "total": {
      "value": 4675,
      "relation": "eq"
    },
    "max_score": 1,
    "hits": []
  }
}
```

The output confirms that `doc['taxful_total_price'].value` is a `java.lang.Double` with the value of `46.96875`. 

This demonstrates the problem: the script attempts to call `toUpperCase()` on a number, which is not supported.

### Step 4: Fix the script

Apply the fix by converting the number to a string first:

```json
GET /kibana_sample_data_ecommerce/_search
{
  "size": 1,
  "query": {
    "match_all": {}
  },
  "script_fields": {
    "formatted_total": {
      "script": {
        "source": """
          // Convert the price to String before applying toUpperCase()
          return "TOTAL: " + String.valueOf(doc['taxful_total_price'].value).toUpperCase();
        """
      }
    }
  }
}
```

### Step 5: Verify the solution works

The corrected script now executes successfully and returns the expected result:

```json
{
  "took": 14,
  "timed_out": false,
  "_shards": {
    "total": 3,
    "successful": 3,
    "skipped": 0,
    "failed": 0
  },
  "hits": {
    "total": {
      "value": 4675,
      "relation": "eq"
    },
    "max_score": 1,
    "hits": [
      {
        "_index": "kibana_sample_data_ecommerce",
        "_id": "z_vDyZgBvpJrRKrKcvig",
        "_score": 1,
        "fields": {
          "formatted_total": [
            "TOTAL: 46.96875"
          ]
        }
      }
    ]
  }
}
```

## Notes

* **Data types in Painless**: Numeric fields in {{es}} are represented as Java numeric types (`Double`, `Integer`), not as `String`.   
* **Field access in Painless**: When accessing a single-valued numeric field like `taxful_total_price`, use `.value` to get the actual numeric value.  
* **Debug.explain() reveals object details**: Shows both the type of object (`java.lang.Double`) and its actual value (`46.96875`), which is useful for understanding how to work with it.  
* **Type conversion**: Convert data types appropriately before applying type-specific methods.
