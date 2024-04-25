/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.services.BuildService;
import org.gradle.api.services.BuildServiceParameters;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class MutedTestsBuildService implements BuildService<MutedTestsBuildService.Params> {
    private final List<String> excludePatterns;

    public MutedTestsBuildService() {
        File infoPath = getParameters().getInfoPath().get().getAsFile();
        File mutedTestsFile = new File(infoPath, "muted-tests.yml");
        try (InputStream is = new BufferedInputStream(new FileInputStream(mutedTestsFile))) {
            ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
            List<MutedTest> mutedTests = objectMapper.readValue(is, MutedTests.class).getTests();
            excludePatterns = buildExcludePatterns(mutedTests == null ? Collections.emptyList() : mutedTests);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public List<String> getExcludePatterns() {
        return excludePatterns;
    }

    private static List<String> buildExcludePatterns(List<MutedTest> mutedTests) {
        List<String> excludes = new ArrayList<>();
        if (mutedTests.isEmpty() == false) {
            for (MutedTestsBuildService.MutedTest mutedTest : mutedTests) {
                if (mutedTest.getClassName() != null && mutedTest.getMethods().isEmpty() == false) {
                    for (String method : mutedTest.getMethods()) {
                        // Tests that use the randomized runner and parameters end up looking like this:
                        // test {yaml=analysis-common/30_tokenizers/letter}
                        // We need to detect this and handle them a little bit different than non-parameterized tests, because of some
                        // quirks in the randomized runner
                        int index = method.indexOf(" {");
                        String methodWithoutParams = index >= 0 ? method.substring(0, index) : method;
                        String paramString = index >= 0 ? method.substring(index) : null;

                        excludes.add(mutedTest.getClassName() + "." + method);

                        if (paramString != null) {
                            // Because of randomized runner quirks, we need skip the test method by itself whenever we want to skip a test
                            // that has parameters
                            // This is because the runner has *two* separate checks that can cause the test to end up getting executed, so
                            // we need filters that cover both checks
                            excludes.add(mutedTest.getClassName() + "." + methodWithoutParams);
                        } else {
                            // We need to add the following, in case we're skipping an entire class of parameterized tests
                            excludes.add(mutedTest.getClassName() + "." + method + " *");
                        }
                    }
                } else if (mutedTest.getClassName() != null) {
                    excludes.add(mutedTest.getClassName() + ".*");
                }
            }
        }

        return excludes;
    }

    public interface Params extends BuildServiceParameters {
        RegularFileProperty getInfoPath();
    }

    public static class MutedTest {
        private final String className;
        private final String method;
        private final List<String> methods;
        private final String issue;

        @JsonCreator
        public MutedTest(
            @JsonProperty("class") String className,
            @JsonProperty("method") String method,
            @JsonProperty("methods") List<String> methods,
            @JsonProperty("issue") String issue
        ) {
            this.className = className;
            this.method = method;
            this.methods = methods;
            this.issue = issue;
        }

        public List<String> getMethods() {
            List<String> allMethods = new ArrayList<>();
            if (methods != null) {
                allMethods.addAll(methods);
            }
            if (method != null) {
                allMethods.add(method);
            }

            return allMethods;
        }

        public String getClassName() {
            return className;
        }

        public String getIssue() {
            return issue;
        }
    }

    private static class MutedTests {
        private final List<MutedTest> tests;

        @JsonCreator
        MutedTests(@JsonProperty("tests") List<MutedTest> tests) {
            this.tests = tests;
        }

        public List<MutedTest> getTests() {
            return tests;
        }
    }
}
