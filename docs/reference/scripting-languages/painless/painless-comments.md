---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-comments.html
products:
  - id: painless
---

# Comments [painless-comments]

Use a comment to annotate or explain code within a script. Use the `//` token anywhere on a line to specify a single-line comment. All characters from the `//` token to the end of the line are ignored. Use an opening `/*` token and a closing `*/` token to specify a multi-line comment. Multi-line comments can start anywhere on a line, and all characters in between the `/*` token and `*/` token are ignored. A comment is included anywhere within a script.

**Grammar**

```text
SINGLE_LINE_COMMENT: '//' .*? [\n\r];
MULTI_LINE_COMMENT: '/*' .*? '*/';
```

**Examples**

* Single-line comments.

    ```painless
    // single-line comment

    int value; // single-line comment
    ```

* Multi-line comments.

    ```painless
    /* multi-
       line
       comment */

    int value; /* multi-
                  line
                  comment */ value = 0;

    int value; /* multi-line
                  comment */

    /* multi-line
       comment */ int value;

    int value; /* multi-line
                  comment */ value = 0;

    int value; /* multi-line comment */ value = 0;
    ```


