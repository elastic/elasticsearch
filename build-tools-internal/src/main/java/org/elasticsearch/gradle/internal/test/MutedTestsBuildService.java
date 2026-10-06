/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.gradle.api.file.RegularFile;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.ListProperty;
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public abstract class MutedTestsBuildService implements BuildService<MutedTestsBuildService.Params> {
    private final List<MutedTest> mutedTests = new ArrayList<>();
    private final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

    public MutedTestsBuildService() {
        File infoPath = getParameters().getInfoPath().get().getAsFile();
        mutedTests.addAll(parseMutedTests(new File(infoPath, "muted-tests.yml")));
        for (RegularFile regularFile : getParameters().getAdditionalFiles().get()) {
            mutedTests.addAll(parseMutedTests(regularFile.getAsFile()));
        }
    }

    public Set<String> getExcludePatternsForTask(String taskPath) {
        Set<String> excludes = new TreeSet<>();
        for (MutedTest mutedTest : mutedTests) {
            if (mutedTest.appliesToTask(taskPath)) {
                addExcludePatterns(mutedTest, excludes);
            }
        }
        return Collections.unmodifiableSet(excludes);
    }

    private List<MutedTest> parseMutedTests(File file) {
        try (InputStream is = new BufferedInputStream(new FileInputStream(file))) {
            MutedTests parsedMutedTests = objectMapper.readValue(is, MutedTests.class);
            if (parsedMutedTests == null || parsedMutedTests.getTests() == null) {
                return Collections.emptyList();
            }
            return parsedMutedTests.getTests();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void addExcludePatterns(MutedTest mutedTest, Set<String> excludes) {
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

    public interface Params extends BuildServiceParameters {
        RegularFileProperty getInfoPath();

        ListProperty<RegularFile> getAdditionalFiles();
    }

    public static class MutedTest {
        private final String className;
        private final String method;
        private final List<String> methods;
        private final String issue;
        private final List<String> tasks;

        @JsonCreator
        public MutedTest(
            @JsonProperty("class") String className,
            @JsonProperty("method") String method,
            @JsonProperty("methods") List<String> methods,
            @JsonProperty("issue") String issue,
            @JsonProperty("tasks") List<?> tasks
        ) {
            this.className = className;
            this.method = method;
            this.methods = methods;
            this.issue = issue;
            this.tasks = validateTasks(tasks);
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

        public boolean appliesToTask(String taskPath) {
            return tasks.isEmpty() || tasks.contains(taskPath);
        }

        private static List<String> validateTasks(List<?> tasks) {
            if (tasks == null) {
                return List.of();
            }
            if (tasks.isEmpty()) {
                throw new IllegalArgumentException("muted test tasks must not be empty");
            }

            Set<String> uniqueTasks = new LinkedHashSet<>();
            for (Object task : tasks) {
                if ((task instanceof String) == false) {
                    throw new IllegalArgumentException("muted test tasks must be strings");
                }
                String taskPath = (String) task;
                if (taskPath.isBlank()) {
                    throw new IllegalArgumentException("muted test tasks must not be blank");
                }
                if (taskPath.startsWith(":") == false) {
                    throw new IllegalArgumentException("muted test tasks must start with ':'");
                }
                uniqueTasks.add(taskPath);
            }

            return uniqueTasks.stream().sorted().collect(Collectors.toUnmodifiableList());
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
