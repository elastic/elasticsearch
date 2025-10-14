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
import org.elasticsearch.gradle.internal.test.rerun.model.TestCase;
import org.elasticsearch.gradle.internal.test.rerun.model.WorkUnit;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.inject.Inject;

public class InternalTestRerunPlugin implements Plugin<Project> {

    public static final String FAILED_TEST_HISTORY_FILENAME = ".failed-test-history.json";
    private final ObjectFactory objectFactory;

    @Inject
    InternalTestRerunPlugin(ObjectFactory objectFactory) {
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

    private static void configureTestTask(Test test, Provider<InternalTestRerunPlugin.RetryTestsBuildService> testsBuildServiceProvider) {
        FailedTestsReport failureReport = testsBuildServiceProvider.get().getFailureReport();
        if (failureReport == null) {
            // no historical test failures found
            return;
        }
        List<WorkUnit> workUnits = failureReport.getWorkUnits();
        Optional<WorkUnit> first = workUnits.stream().filter(workunit -> workunit.getName().equals(test.getPath())).findFirst();
        if (first.isPresent()) {
            test.filter(testFilter -> {
                List<String> includes = new ArrayList<>();
                List<TestCase> tests = first.get().getTests();
                for (TestCase testClassCase : tests) {
                    List<TestCase> children = testClassCase.getChildren();
                    for (TestCase child : children) {
                        testFilter.includeTest(testClassCase.getName(), child.getName());
                    }
                }
            });
        } else {
            test.onlyIf("Ignoring Test Task during in rerun. ", element -> false);
        }
    }

    public abstract static class RetryTestsBuildService implements BuildService<RetryTestsBuildService.Params> {

        private final FailedTestsReport failureReport;

        interface Params extends BuildServiceParameters {
            RegularFileProperty getInfoPath();
        }

        public RetryTestsBuildService() {
            File failedTestsJsonFile = new File(getParameters().getInfoPath().getAsFile().get(), FAILED_TEST_HISTORY_FILENAME);
            if (failedTestsJsonFile.exists()) {
                try {
                    ObjectMapper objectMapper = new ObjectMapper();
                    this.failureReport = objectMapper.readValue(failedTestsJsonFile, FailedTestsReport.class);
                } catch (IOException e) {
                    throw new RuntimeException(String.format("Failed to parse %s", FAILED_TEST_HISTORY_FILENAME), e);
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
