/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle.internal.doc;

import org.elasticsearch.gradle.OS;
import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.VersionProperties;
import org.gradle.api.Plugin;
import org.gradle.api.plugins.PluginManager;
import org.gradle.api.Project;
import org.gradle.api.file.Directory;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.TaskProvider;

import java.util.Map;
import java.util.HashMap;

/**
 * Sets up tests for documentation.
 */
class DocsTestPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        applyPlugins(project);

        String distribution = System.getProperty('tests.distribution', 'default');
        // The distribution can be configured with -Dtests.distribution on the command line
        project.property("testClusters").matching { it.name.equals("integTest") }.configureEach { testDistribution = distribution.toUpperCase() }
        project.property("testClusters").matching { it.name.equals("integTest") }.configureEach { nameCustomization = { it.replace("integTest", "node") } }

        // Docs are published separately so no need to assemble
        project.getTasks().named("assemble").configure {enabled = false }

        Map<String, String> commonDefaultSubstitutions = makeDefaultSubstitutionMap();
        project.getTasks().register('listSnippets', SnippetsTask) {
            group 'Docs'
            description 'List each snippet'
            defaultSubstitutions = commonDefaultSubstitutions
            perSnippet { println(it.toString()) }
        }
        project.getTasks().register('listConsoleCandidates', SnippetsTask) {
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

        Provider<Directory> restRootDir = project.getLayout().getBuildDirectory().dir("rest");
        TaskProvider<RestTestsFromSnippetsTask> buildRestTests = project.getTasks().register('buildRestTests', RestTestsFromSnippetsTask) {
            defaultSubstitutions = commonDefaultSubstitutions
            testRoot.convention(restRootDir)
        }

        // TODO: This effectively makes testRoot not customizable, which we don't do anyway atm
        project.property("sourceSets").getByName("test").getOutput().dir(restRootDir, builtBy: buildRestTests)

    }

    private void applyPlugins(Project proj) {
        PluginManager pm = proj.getPluginManager();
        pm.apply("elasticsearch.internal-testclusters");
        pm.apply("elasticsearch.standalone-rest-test");
        pm.apply("elasticsearch.rest-test");
    }

    private Map<String,String> makeDefaultSubstitutionMap() {
        Map<String,String> result = new HashMap<String,String>();
        result.put("\\{version\\}", Version.fromString(VersionProperties.getElasticsearch()).toString());
        result.put("\\{version_qualified\\}", VersionProperties.getElasticsearch());
        result.put("\\{build_flavor\\}", System.getProperty("tests.distribution", "default"));
        result.put("\\{lucene_version\\}", VersionProperties.getLucene().replaceAll("\\-snapshot-\\w+",""));

        // This last one still needs to be adapted for java
        result.put("\\{build_type\\}", OS.conditionalString().onWindows({"zip"}).onUnix({"tar"}).supply());

        return result;
    }

}
