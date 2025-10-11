/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test.rerun;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.elasticsearch.gradle.internal.test.rerun.model.FailedTestsReport;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.provider.Provider;
import org.gradle.api.services.BuildService;
import org.gradle.api.services.BuildServiceParameters;
import org.gradle.api.tasks.testing.Test;

import java.io.File;
import java.io.IOException;

import javax.inject.Inject;

import static org.elasticsearch.gradle.internal.test.rerun.TestTaskConfigurer.configureTestTask;

public class RerunPlugin implements Plugin<Project> {

    private final ObjectFactory objectFactory;

    @Inject
    RerunPlugin(ObjectFactory objectFactory) {
        this.objectFactory = objectFactory;

    }

    @Override
    public void apply(Project project) {
        File settingsRoot = project.getLayout().getSettingsDirectory().getAsFile();

        Provider<RetryTestsBuildService> retryTestsProvider = project.getGradle()
            .getSharedServices()
            .registerIfAbsent("retryTests", RetryTestsBuildService.class, spec -> {
                spec.getParameters().getInfoPath().set(settingsRoot);
            });

        project.getTasks().withType(Test.class).configureEach(task -> configureTestTask(task, retryTestsProvider));

    }

    public abstract static class RetryTestsBuildService implements BuildService<RetryTestsBuildService.Params> {

        private final FailedTestsReport failureReport;

        interface Params extends BuildServiceParameters {
            RegularFileProperty getInfoPath();
        }

        public RetryTestsBuildService() {
            File failedTestsJsonFile = new File(getParameters().getInfoPath().getAsFile().get(), "failed-tests.json");
            if (failedTestsJsonFile.exists()) {
                try {
                    ObjectMapper objectMapper = new ObjectMapper();
                    this.failureReport = objectMapper.readValue(failedTestsJsonFile, FailedTestsReport.class);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to parse retry-tests.json", e);
                }
            } else {
                this.failureReport = null;
            }
        }

        public FailedTestsReport getFailureReport() {
            return failureReport;
        }
    }
}
