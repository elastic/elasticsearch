/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.coverage;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.testing.jacoco.plugins.JacocoPlugin;
import org.gradle.testing.jacoco.plugins.JacocoPluginExtension;
import org.gradle.testing.jacoco.tasks.JacocoReport;

public class TransportClassesCoveragePlugin implements Plugin<Project> {
    private static final Logger LOGGER = Logging.getLogger(TransportClassesCoveragePlugin.class);

    @Override
    public void apply(Project project) {
        project.getPluginManager().withPlugin("elasticsearch.build", plugin -> {
            // TODO a workaround until 0.8.9 jacoco with jdk20 support is released
            project.getRepositories().maven(maven -> { maven.setUrl("https://oss.sonatype.org/content/repositories/snapshots/"); });

            project.getPluginManager().apply(JacocoPlugin.class);

            // support for java 20
            project.getExtensions().getByType(JacocoPluginExtension.class).setToolVersion("0.8.9-20230327.073737-36");

            // this is suggested by gradle jacoco doc https://docs.gradle.org/current/userguide/jacoco_plugin.html
            project.getTasks().named("test").configure(task -> { task.finalizedBy(project.getTasks().named("jacocoTestReport")); });
            TaskProvider<JacocoReport> jacocoTestReport = project.getTasks().named("jacocoTestReport", JacocoReport.class);

            jacocoTestReport.configure(task -> {
                task.reports(reports -> reports.getXml().getRequired().set(true));
                task.dependsOn(project.getTasks().named("test"));
            });

            TaskProvider<TransportMethodCoverageVerifierTask> methodCoverageVerifier = project.getTasks()
                .register("transportMethodCoverageVerifier", TransportMethodCoverageVerifierTask.class);
            methodCoverageVerifier.configure(task -> { task.dependsOn(jacocoTestReport); });

            project.getTasks().named("check").configure(task -> task.dependsOn(methodCoverageVerifier));

        });
    }

}
