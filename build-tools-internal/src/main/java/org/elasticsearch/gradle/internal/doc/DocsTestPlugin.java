/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.doc;

import org.elasticsearch.gradle.OS;
import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.testclusters.ElasticsearchCluster;
import org.elasticsearch.gradle.testclusters.TestClustersPlugin;
import org.elasticsearch.gradle.testclusters.TestDistribution;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.file.Directory;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.internal.file.FileOperations;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.TaskProvider;

import java.util.Map;

import javax.inject.Inject;

public class DocsTestPlugin implements Plugin<Project> {
    private FileOperations fileOperations;
    private ProjectLayout projectLayout;

    @Inject
    DocsTestPlugin(FileOperations fileOperations, ProjectLayout projectLayout) {
        this.projectLayout = projectLayout;
        this.fileOperations = fileOperations;
    }

    @Override
    public void apply(Project project) {
        project.getPluginManager().apply("elasticsearch.legacy-yaml-rest-test");

        String distribution = System.getProperty("tests.distribution", "default");
        // The distribution can be configured with -Dtests.distribution on the command line
        NamedDomainObjectContainer<ElasticsearchCluster> testClusters = (NamedDomainObjectContainer<ElasticsearchCluster>) project
            .getExtensions()
            .getByName(TestClustersPlugin.EXTENSION_NAME);

        testClusters.matching((c) -> c.getName().equals("yamlRestTest")).configureEach(c -> {
            c.setTestDistribution(TestDistribution.valueOf(distribution.toUpperCase()));
            c.setNameCustomization((name) -> name.replace("yamlRestTest", "node"));
        });

        project.getTasks().named("assemble").configure(task -> { task.setEnabled(false); });

        Map<String, String> commonDefaultSubstitutions = Map.of(
            /* These match up with the asciidoc syntax for substitutions but
             * the values may differ. In particular {version} needs to resolve
             * to the version being built for testing but needs to resolve to
             * the last released version for docs. */
            "\\{version\\}",
            Version.fromString(VersionProperties.getElasticsearch()).toString(),
            "\\{version_qualified\\}",
            VersionProperties.getElasticsearch(),
            "\\{lucene_version\\}",
            VersionProperties.getLucene().replaceAll("-snapshot-\\w+$", ""),
            "\\{build_flavor\\}",
            distribution,
            "\\{build_type\\}",
            OS.conditionalString().onWindows(() -> "zip").onUnix(() -> "tar").supply()
        );

        project.getTasks().register("listSnippets", DocSnippetTask.class, task -> {
            task.setGroup("Docs");
            task.setDescription("List each snippet");
            task.getDefaultSubstitutions().putAll(commonDefaultSubstitutions);
            task.setPerSnippet(System.out::println);
        });

        project.getTasks().register("listConsoleCandidates", DocSnippetTask.class, task -> {
            task.setGroup("Docs");
            task.setDescription("List snippets that probably should be marked // CONSOLE");
            task.getDefaultSubstitutions().putAll(commonDefaultSubstitutions);
            task.setPerSnippet(snippet -> {
                if (snippet.isConsoleCandidate()) {
                    System.out.println(snippet);
                }
            });
        });

        Provider<Directory> restRootDir = projectLayout.getBuildDirectory().dir("rest");
        TaskProvider<RestTestsFromDocSnippetTask> buildRestTests = project.getTasks()
            .register("buildRestTests", RestTestsFromDocSnippetTask.class, task -> {
                task.getDefaultSubstitutions().putAll(commonDefaultSubstitutions);
                task.getTestRoot().convention(restRootDir);
                task.getMigrationMode().set(Boolean.getBoolean("gradle.docs.migration"));
                task.doFirst(task1 -> fileOperations.delete(restRootDir.get()));
            });

        // TODO: This effectively makes testRoot not customizable, which we don't do anyway atm
        JavaPluginExtension byType = project.getExtensions().getByType(JavaPluginExtension.class);
        byType.getSourceSets().getByName("yamlRestTest").getOutput().dir(Map.of("builtBy", buildRestTests), restRootDir);
    }

}
