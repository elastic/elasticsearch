/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.doc

import org.elasticsearch.gradle.fixtures.AbstractGradleInternalPluginFuncTest
import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitPlugin
import org.gradle.testkit.runner.TaskOutcome

class DocsTestPluginFuncTest extends AbstractGradleInternalPluginFuncTest {
    Class<? extends PrecommitPlugin> pluginClassUnderTest = DocsTestPlugin.class

    def setup() {
        File docDir = new File(projectDir, 'doc');
        docDir.mkdirs()
        addSampleDoc(docDir)
        buildApiRestrictionsDisabled = true
    configurationCacheCompatible = false;
        buildFile << """
tasks.named('listSnippets') {
   docs = fileTree('doc')
}

tasks.named('listConsoleCandidates') {
   docs = fileTree('doc')
}
"""
    }

    def "can list snippets"() {
        when:
        def result = gradleRunner("listSnippets").build()
        then:
        result.task(":listSnippets").outcome == TaskOutcome.SUCCESS
        assertOutputContains(result.output, """
> Task :listSnippets
mapper-annotated-text.asciidoc[37:39](Painless)
mapper-annotated-text.asciidoc[42:44](js)
mapper-annotated-text.asciidoc[51:69](console)// TEST[setup:seats]
""")
    }

    def "can list console candidates"() {
        when:
        def result = gradleRunner("listConsoleCandidates").build()
        then:
        result.task(":listConsoleCandidates").outcome == TaskOutcome.SUCCESS
        assertOutputContains(result.output, """
> Task :listConsoleCandidates
mapper-annotated-text.asciidoc[42:44](js)
""")
    }

    void addSampleDoc(File docFolder) {
        new File(docFolder, "mapper-annotated-text.asciidoc").text = """
[[painless-filter-context]]
=== Filter context

Use a Painless script as a {ref}/query-dsl-script-query.html[filter] in a
query to include and exclude documents.


*Variables*

`params` (`Map`, read-only)::
        User-defined parameters passed in as part of the query.

`doc` (`Map`, read-only)::
        Contains the fields of the current document where each field is a
        `List` of values.

*Return*

`boolean`::
        Return `true` if the current document should be returned as a result of
        the query, and `false` otherwise.


*API*

The standard <<painless-api-reference-shared, Painless API>> is available.

*Example*

To run this example, first follow the steps in
<<painless-context-examples, context examples>>.

This script finds all unsold documents that cost less than \$25.

[source,Painless]
----
doc['sold'].value == false && doc['cost'].value < 25
----

[source,js]
----
curl 'hello world'
----

Defining `cost` as a script parameter enables the cost to be configured
in the script query request. For example, the following request finds
all available theatre seats for evening performances that are under \$25.

[source,console]
----
GET seats/_search
{
  "query": {
    "bool": {
      "filter": {
        "script": {
          "script": {
            "source": "doc['sold'].value == false && doc['cost'].value < params.cost",
            "params": {
              "cost": 25
            }
          }
        }
      }
    }
  }
}
----
// TEST[setup:seats]
"""
    }
}
