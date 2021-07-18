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
import org.elasticsearch.gradle.testclusters.ElasticsearchCluster;
import org.elasticsearch.gradle.testclusters.TestDistribution;
import org.gradle.api.Plugin;
import org.gradle.api.plugins.PluginManager;
import org.gradle.api.Project;
import org.gradle.api.file.Directory;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.NamedDomainObjectCollection;

import java.util.Map;
import java.util.HashMap;

/**
 * Sets up tests for documentation.
 */
public class DocsTestPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        applyPlugins(project);
        
        TestDistribution distribution = getTestDistribution(System.getProperty("tests.distribution", "default"));
        // The distribution can be configured with -Dtests.distribution on the command line
        NamedDomainObjectCollection<ElasticsearchCluster> testClustersCollection = (NamedDomainObjectCollection<ElasticsearchCluster>) project.property("testClusters");
        NamedDomainObjectCollection<ElasticsearchCluster> integTestCollection = testClustersCollection.matching( ec -> ec.getName().equals("integTest"));
        integTestCollection.configureEach(ec -> ec.setTestDistribution(distribution));
        integTestCollection.configureEach(ec -> ec.setNameCustomization( nc -> nc.replace("integTest", "node")));

        // Docs are published separately so no need to assemble

        project.getTasks().named("assemble").configure(task -> task.setEnabled(false));
        Map<String, String> commonDefaultSubstitutions = makeDefaultSubstitutionMap();        

        project.getTasks().register("listSnippets", SnippetsTask.class , task -> {
            task.setGroup("Docs");
            task.setDescription("List each snippet");
            task.setDefaultSubstitutions(commonDefaultSubstitutions);
            task.setPerSnippetAction(snippet -> System.out.println(snippet.toString()));
        });


        project.getTasks().register("listConsoleCandidates", SnippetsTask.class, task -> {
            task.setGroup("Docs");
            task.setDescription("List snippets that probably should be marked // CONSOLE");
            task.setDefaultSubstitutions(commonDefaultSubstitutions);
            task.setPerSnippetAction(snippet -> {
                if (RestTestsFromSnippetsTask.isConsoleCandidate(snippet)) {
                    System.out.println(snippet.toString());
                }

            });
        });

        Provider<Directory> restRootDir = project.getLayout().getBuildDirectory().dir("rest");

        TaskProvider<RestTestsFromSnippetsTask> buildRestTests = project.getTasks().register("buildRestTests", RestTestsFromSnippetsTask.class, task -> {
            task.setDefaultSubstitutions(commonDefaultSubstitutions);
            task.getTestRoot().convention(restRootDir);
        });

        // TODO: This effectively makes testRoot not customizable, which we don't do anyway atm
        Map<String,Object> dirOptions = new HashMap<>();
        dirOptions.put("builtBy", buildRestTests);
        SourceSetContainer sourceSets = (SourceSetContainer) project.property("sourceSets");
        sourceSets.getByName("test").getOutput().dir(dirOptions,restRootDir);
    }

    private void applyPlugins(Project proj) {
        PluginManager pm = proj.getPluginManager();
        pm.apply("elasticsearch.internal-testclusters");
        pm.apply("elasticsearch.standalone-rest-test");
        pm.apply("elasticsearch.rest-test");
    }

    private TestDistribution getTestDistribution(String distString) {
        TestDistribution result = TestDistribution.DEFAULT;

        if (distString.toUpperCase().equals("INTEG_TEST"))
            result = TestDistribution.INTEG_TEST;

        return result;
    }

    private Map<String,String> makeDefaultSubstitutionMap() {
        Map<String,String> result = new HashMap<String,String>();

        /* These match up with the asciidoc syntax for substitutions but
        * the values may differ. In particular {version} needs to resolve
        * to the version being built for testing but needs to resolve to
        * the last released version for docs. */
        result.put("\\{version\\}", Version.fromString(VersionProperties.getElasticsearch()).toString());
        result.put("\\{version_qualified\\}", VersionProperties.getElasticsearch());
        result.put("\\{build_flavor\\}", System.getProperty("tests.distribution", "default"));
        result.put("\\{lucene_version\\}", VersionProperties.getLucene().replaceAll("\\-snapshot-\\w+",""));
        result.put("\\{build_type\\}", OS.conditionalString().onWindows(() -> "zip").onUnix(() -> "tar").supply());

        return result;
    }
}
