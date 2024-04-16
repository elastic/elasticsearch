/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.doc

class MdxSnippetParserSpec extends AbstractParserSpec {

    @Override
    SnippetParser parser(Map<String,String> defaultSubstitutions = [:]) {
        return new MdxSnippetParser(defaultSubstitutions)
    }

    @Override
    protected String docSnippetWithTestSetup() {
        return """
```console
GET seats/_search
{
  "query": {
    "match_all": {}
  }
}
```
{/* TEST[setup:seats] */}

"""
    }
}
