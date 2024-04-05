/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.docs;

import org.elasticsearch.gradle.OS;
import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.testclusters.ElasticsearchCluster;
import org.elasticsearch.gradle.testclusters.TestClustersPlugin;
import org.elasticsearch.gradle.testclusters.TestDistribution;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.internal.file.FileOperations;

import java.util.Map;

import javax.inject.Inject;

public class Docs2Plugin implements Plugin<Project> {
    private FileOperations fileOperations;
    private ProjectLayout projectLayout;

    @Inject
    Docs2Plugin(FileOperations fileOperations, ProjectLayout projectLayout) {
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
            task.setDefaultSubstitutions(commonDefaultSubstitutions);
            task.setPerSnippet(snippet -> System.out.println(snippet.toString()));
        });

        project.getTasks().register("listConsoleCandidates", DocSnippetTask.class, task -> {
            task.setGroup("Docs");
            task.setDescription("List snippets that probably should be marked // CONSOLE");
            task.setDefaultSubstitutions(commonDefaultSubstitutions);
            task.setPerSnippet(snippet -> {
                if (isConsoleCandidate(snippet)) {
                    System.out.println(snippet.toString());
                }
            });
        });
    }

    /**
     * Is this snippet a candidate for conversion to `// CONSOLE`?
     */
    private static boolean isConsoleCandidate(Snippet snippet) {
        /* Snippets that are responses or already marked as `// CONSOLE` or
         * `// NOTCONSOLE` are not candidates. */
        if (snippet.console != null || snippet.testResponse) {
            return false;
        }
        /* js snippets almost always should be marked with `// CONSOLE`. js
         * snippets that shouldn't be marked `// CONSOLE`, like examples for
         * js client, should always be marked with `// NOTCONSOLE`.
         *
         * `sh` snippets that contain `curl` almost always should be marked
         * with `// CONSOLE`. In the exceptionally rare cases where they are
         * not communicating with Elasticsearch, like the examples in the ec2
         * and gce discovery plugins, the snippets should be marked
         * `// NOTCONSOLE`. */
        return snippet.language.equals("js") || snippet.curl;
    }
}
