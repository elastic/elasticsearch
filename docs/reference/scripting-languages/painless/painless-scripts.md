---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-scripts.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Scripts [painless-scripts]

A script in Painless is a complete program composed of one or more [statements](/reference/scripting-languages/painless/painless-statements.md) that performs a specific task within {{es}}. Scripts run within a controlled sandbox environment that determines which variables are available and which APIs can be accessed based on the [use context](docs-content://explore-analyze/scripting/painless-syntax-context-bridge.md). 

Scripts are the fundamental runnable unit in Painless, allowing you to customize⁣ {{es}} behavior for search scoring, data transformation, field calculations, and operational tasks. Each script runs in isolation with access to context-specific variables and a curated set of safe operations.

For detailed guidance on writing scripts, practical examples, and step-by-step tutorials, refer to [How to write Painless scripts](docs-content://explore-analyze/scripting/modules-scripting-using.md). 
