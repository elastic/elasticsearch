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
import org.gradle.api.attributes.TestSuiteType;
import org.gradle.api.reporting.ReportingExtension;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.testing.jacoco.plugins.JacocoCoverageReport;
import org.gradle.testing.jacoco.plugins.JacocoReportAggregationPlugin;

public class ESJacocoReportAggregation implements Plugin<Project> {
    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(JacocoReportAggregationPlugin.class);
        project.getPluginManager().apply(RepositoriesSetupPlugin.class);

        project.getExtensions().configure(ReportingExtension.class, reports -> {
            reports.getReports().register("testCodeCoverageReport", JacocoCoverageReport.class).configure(report -> {
                report.getTestType().set(TestSuiteType.UNIT_TEST);
            });
        });

        TaskProvider<TransportMethodCoverageVerifierTask> methodCoverageVerifier = project.getTasks()
            .register("transportMethodCoverageVerifier", TransportMethodCoverageVerifierTask.class);
        methodCoverageVerifier.configure(task -> { task.dependsOn("testCodeCoverageReport"); });

        project.getTasks().named("check").configure(task -> task.dependsOn(methodCoverageVerifier));

    }
}
