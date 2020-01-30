/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.gradle.doc

import org.elasticsearch.gradle.OS
import org.elasticsearch.gradle.Version
import org.elasticsearch.gradle.VersionProperties
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.Task

/**
 * Sets up tests for documentation.
 */
class DocsTestPlugin implements Plugin<Project> {

    @Override
    void apply(Project project) {
        project.pluginManager.apply('elasticsearch.testclusters')
        project.pluginManager.apply('elasticsearch.standalone-rest-test')
        project.pluginManager.apply('elasticsearch.rest-test')

        String distribution = System.getProperty('tests.distribution', 'default')
        // The distribution can be configured with -Dtests.distribution on the command line
        project.testClusters.integTest.testDistribution = distribution.toUpperCase()
        project.testClusters.integTest.nameCustomization = { it.replace("integTest", "node") }
        // Docs are published separately so no need to assemble
        project.tasks.assemble.enabled = false
        Map<String, String> defaultSubstitutions = [
                /* These match up with the asciidoc syntax for substitutions but
                 * the values may differ. In particular {version} needs to resolve
                 * to the version being built for testing but needs to resolve to
                 * the last released version for docs. */
            '\\{version\\}': Version.fromString(VersionProperties.elasticsearch).toString(),
            '\\{version_qualified\\}': VersionProperties.elasticsearch,
            '\\{lucene_version\\}' : VersionProperties.lucene.replaceAll('-snapshot-\\w+$', ''),
            '\\{build_flavor\\}' : distribution,
            '\\{build_type\\}' : OS.conditionalString().onWindows({"zip"}).onUnix({"tar"}).supply(),
        ]
        Task listSnippets = project.tasks.create('listSnippets', SnippetsTask)
        listSnippets.group 'Docs'
        listSnippets.description 'List each snippet'
        listSnippets.defaultSubstitutions = defaultSubstitutions
        listSnippets.perSnippet { println(it.toString()) }

        Task listConsoleCandidates = project.tasks.create(
                'listConsoleCandidates', SnippetsTask)
        listConsoleCandidates.group 'Docs'
        listConsoleCandidates.description
                'List snippets that probably should be marked // CONSOLE'
        listConsoleCandidates.defaultSubstitutions = defaultSubstitutions
        listConsoleCandidates.perSnippet {
            if (RestTestsFromSnippetsTask.isConsoleCandidate(it)) {
                println(it.toString())
            }
        }

        Task buildRestTests = project.tasks.create(
                'buildRestTests', RestTestsFromSnippetsTask)
        buildRestTests.defaultSubstitutions = defaultSubstitutions
    }
}
