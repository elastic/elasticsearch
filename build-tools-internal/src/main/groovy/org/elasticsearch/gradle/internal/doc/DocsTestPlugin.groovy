/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle.internal.doc

import org.elasticsearch.gradle.OS
import org.elasticsearch.gradle.Version
import org.elasticsearch.gradle.VersionProperties
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.file.Directory
import org.gradle.api.provider.Provider
import org.gradle.api.tasks.TaskProvider

/**
 * Sets up tests for documentation.
 */
class DocsTestPlugin implements Plugin<Project> {

    @Override
    void apply(Project project) {
        project.pluginManager.apply('elasticsearch.internal-testclusters')
        project.pluginManager.apply('elasticsearch.standalone-rest-test')
        project.pluginManager.apply('elasticsearch.rest-test')

        String distribution = System.getProperty('tests.distribution', 'default')
        // The distribution can be configured with -Dtests.distribution on the command line
        project.testClusters.matching { it.name.equals("integTest") }.configureEach { testDistribution = distribution.toUpperCase() }
        project.testClusters.matching { it.name.equals("integTest") }.configureEach { nameCustomization = { it.replace("integTest", "node") } }
        // Docs are published separately so no need to assemble
        project.tasks.named("assemble").configure {enabled = false }
        Map<String, String> commonDefaultSubstitutions = [
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
        project.tasks.register('listSnippets', SnippetsTask) {
            group 'Docs'
            description 'List each snippet'
            defaultSubstitutions = commonDefaultSubstitutions
            perSnippet { println(it.toString()) }
        }
        project.tasks.register('listConsoleCandidates', SnippetsTask) {
            group 'Docs'
            description
            'List snippets that probably should be marked // CONSOLE'
            defaultSubstitutions = commonDefaultSubstitutions
            perSnippet {
                if (RestTestsFromSnippetsTask.isConsoleCandidate(it)) {
                    println(it.toString())
                }
            }
        }

        Provider<Directory> restRootDir = project.getLayout().buildDirectory.dir("rest")
        TaskProvider<RestTestsFromSnippetsTask> buildRestTests = project.tasks.register('buildRestTests', RestTestsFromSnippetsTask) {
            defaultSubstitutions = commonDefaultSubstitutions
            testRoot.convention(restRootDir)
        }

        // TODO: This effectively makes testRoot not customizable, which we don't do anyway atm
        project.sourceSets.test.output.dir(restRootDir, builtBy: buildRestTests)
    }
}
