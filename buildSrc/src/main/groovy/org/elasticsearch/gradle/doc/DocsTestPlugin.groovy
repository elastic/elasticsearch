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

import org.elasticsearch.gradle.VersionProperties
import org.elasticsearch.gradle.test.RestTestPlugin
import org.gradle.api.Project
import org.gradle.api.Task

/**
 * Sets up tests for documentation.
 */
public class DocsTestPlugin extends RestTestPlugin {

    @Override
    public void apply(Project project) {
        project.pluginManager.apply('elasticsearch.standalone-rest-test')
        super.apply(project)
        // Docs are published separately so no need to assemble
        project.tasks.remove(project.assemble)
        project.build.dependsOn.remove('assemble')
        Map<String, String> defaultSubstitutions = [
            /* These match up with the asciidoc syntax for substitutions but
             * the values may differ. In particular {version} needs to resolve
             * to the version being built for testing but needs to resolve to
             * the last released version for docs. */
            '\\{version\\}':
                VersionProperties.elasticsearch.replace('-SNAPSHOT', ''),
            '\\{lucene_version\\}' : VersionProperties.lucene.replaceAll('-snapshot-\\w+$', ''),
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
