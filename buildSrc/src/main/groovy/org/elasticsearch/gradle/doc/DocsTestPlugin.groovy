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

import org.elasticsearch.gradle.test.RestTestPlugin
import org.gradle.api.Project
import org.gradle.api.Task

/**
 * Sets up tests for documentation.
 */
public class DocsTestPlugin extends RestTestPlugin {

    @Override
    public void apply(Project project) {
        super.apply(project)
        Task listSnippets = project.tasks.create('listSnippets', SnippetsTask)
        listSnippets.group 'Docs'
        listSnippets.description 'List each snippet'
        listSnippets.perSnippet { println(it.toString()) }

        Task listConsoleCandidates = project.tasks.create(
                'listConsoleCandidates', SnippetsTask)
        listConsoleCandidates.group 'Docs'
        listConsoleCandidates.description
                'List snippets that probably should be marked // CONSOLE'
        listConsoleCandidates.perSnippet {
            if (
                       it.console      // Already marked, nothing to do
                    || it.testResponse // It is a response
                ) {
                return
            }
            List<String> languages = [
                // These languages should almost always be marked console
                'js', 'json',
                // These are often curl commands that should be converted but
                // are probably false positives
                'sh', 'shell',
            ]
            if (false == languages.contains(it.language)) {
                return
            }
            println(it.toString())
        }

        project.tasks.create('buildRestTests', RestTestsFromSnippetsTask)
    }
}
