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
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.elasticsearch.gradle.internal.test.rerun.model.FailedTestsReport;
import org.elasticsearch.gradle.internal.test.rerun.model.TestCase;
import org.elasticsearch.gradle.internal.test.rerun.model.WorkUnit;
import org.gradle.api.file.Directory;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.testing.Test;
import org.gradle.api.tasks.testing.TestDescriptor;
import org.gradle.api.tasks.testing.TestListener;
import org.gradle.api.tasks.testing.TestResult;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public final class TestTaskConfigurer {

    private TestTaskConfigurer() {}

    public static void configureTestTask(Test test, Provider<RerunPlugin.RetryTestsBuildService> testsBuildServiceProvider) {
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
                    if (children.size() == 0) {
                        // likely test setup has failed. just include the whole class
                        includes.add(testClassCase.getName());
                    } else {
                        for (TestCase child : children) {
                            // TODO sync and reuse whats in MutedTestService here
                            // Tests that use the randomized runner and parameters end up looking like this:
                            // test {yaml=analysis-common/30_tokenizers/letter}
                            // We need to detect this and handle them a little bit different than non-parameterized tests, because of some
                            // quirks in the randomized runner
                            String method = child.getName();
                            int index = method.indexOf(" {");
                            String methodWithoutParams = index >= 0 ? method.substring(0, index) : method;
                            String paramString = index >= 0 ? method.substring(index) : null;

                            includes.add(testClassCase.getName() + "." + method);

                            if (paramString != null) {
                                // Because of randomized runner quirks, we need skip the test method by itself whenever we want to skip a
                                // test
                                // that has parameters
                                // This is because the runner has *two* separate checks that can cause the test to end up getting executed,
                                // so
                                // we need filters that cover both checks
                                includes.add(testClassCase.getName() + "." + methodWithoutParams);
                            } else {
                                // We need to add the following, in case we're skipping an entire class of parameterized tests
                                includes.add(testClassCase.getName() + "." + method + " *");
                            }
                        }
                        for (String include : includes) {
                            testFilter.includeTestsMatching(include);
                        }
                    }
                }

            });
        } else {
            test.onlyIf("Ignoring Test Task during in rerun. ", element -> false);
        }
    }

    private static class TrackFailedTestListener implements TestListener {

        private final Directory projectDirectory;
        private final String taskName;
        private final List<TestDescriptor> failedTests = new ArrayList<>();

        TrackFailedTestListener(String taskName, Directory projectDirectory) {
            this.taskName = taskName;
            this.projectDirectory = projectDirectory;
        }

        @Override
        public void beforeSuite(TestDescriptor suite) {}

        @Override
        public void afterSuite(TestDescriptor suite, TestResult result) {
            if (suite.getParent() == null && failedTests.isEmpty() == false) {
                File failedTestsFile = new File(projectDirectory.getAsFile(), "build/test-rerun/" + taskName + "-failed-tests.yml");
                failedTestsFile.getParentFile().mkdirs();
                ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
                try {
                    objectMapper.writeValue(
                        failedTestsFile,
                        failedTests.stream()
                            .map(testDescriptor -> testDescriptor.getClassName() + "." + testDescriptor.getName())
                            .collect(Collectors.toList())
                    );
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        @Override
        public void beforeTest(TestDescriptor testDescriptor) {}

        @Override
        public void afterTest(TestDescriptor testDescriptor, TestResult result) {
            if (result.getResultType() == TestResult.ResultType.FAILURE) {
                failedTests.add(testDescriptor);
            }
        }
    }
}
