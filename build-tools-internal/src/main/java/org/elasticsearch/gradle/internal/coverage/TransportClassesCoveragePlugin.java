/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.coverage;

import org.elasticsearch.gradle.internal.RepositoriesSetupPlugin;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.attributes.TestSuiteType;
import org.gradle.api.reporting.ReportingExtension;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.testing.jacoco.plugins.JacocoCoverageReport;
import org.gradle.testing.jacoco.plugins.JacocoPlugin;
import org.gradle.testing.jacoco.plugins.JacocoPluginExtension;
import org.gradle.testing.jacoco.plugins.JacocoReportAggregationPlugin;
import org.gradle.testing.jacoco.tasks.JacocoReport;

public class TransportClassesCoveragePlugin implements Plugin<Project> {
    @Override
    public void apply(Project project) {

        project.getPluginManager().apply(JacocoPlugin.class);
        project.getPluginManager().apply(JacocoReportAggregationPlugin.class);
        project.getPluginManager().apply(RepositoriesSetupPlugin.class);

        project.getExtensions().configure(ReportingExtension.class, reports -> {
            reports.getReports().register("testCodeCoverageReport", JacocoCoverageReport.class).configure(report -> {
                report.getTestType().set(TestSuiteType.UNIT_TEST);
            });
            // the test is failing .. but the 'real' run needs the above
            // reports.getReports().withType(JacocoCoverageReport.class)
            // .all(report -> {
            // report.getTestType().set(TestSuiteType.UNIT_TEST);
            // });
        });

        TaskProvider<TransportMethodCoverageVerifierTask> methodCoverageVerifier = project.getTasks()
            .register("transportMethodCoverageVerifier", TransportMethodCoverageVerifierTask.class);
        methodCoverageVerifier.configure(task -> { task.dependsOn("testCodeCoverageReport"); });

//        project.getTasks().named("check").configure(task -> task.dependsOn(methodCoverageVerifier));

        for (Project subproject : project.getSubprojects()) {
            configureSubproject(subproject);
        }

    }

    private static void configureSubproject(Project subproject) {
        subproject.afterEvaluate(project -> {
            if (project.getDisplayName().contains("test") == false) {
                project.getPluginManager().withPlugin("elasticsearch.build", plugin -> {

                    project.getPluginManager().apply(JacocoPlugin.class);

                    // support for java 20
                    project.getExtensions().getByType(JacocoPluginExtension.class).setToolVersion("0.8.9");

                    TaskProvider<JacocoReport> jacocoTestReport = project.getTasks().named("jacocoTestReport", JacocoReport.class);

                    jacocoTestReport.configure(task -> {
                        task.reports(reports -> reports.getXml().getRequired().set(true));

                        task.dependsOn(project.getTasks().named("test"));
                        Task internalClusterTest = project.getTasks().findByName("internalClusterTest");
                        if (internalClusterTest != null) {
                            task.dependsOn(internalClusterTest);
                        }
                    });

                    project.getRootProject().getTasks().named("testCodeCoverageReport").configure(t -> t.dependsOn(jacocoTestReport));

                    project.getRootProject().getDependencies().add("jacocoAggregation", project);
                });
            }
        });

    }
}
