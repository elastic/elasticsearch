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

public abstract class MutedTestsBuildService implements BuildService<MutedTestsBuildService.Params> {
    private final Set<String> excludePatterns = new LinkedHashSet<>();
    private final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

    public MutedTestsBuildService() {
        File infoPath = getParameters().getInfoPath().get().getAsFile();
        File mutedTestsFile = new File(infoPath, "muted-tests.yml");
        excludePatterns.addAll(buildExcludePatterns(mutedTestsFile));
        for (RegularFile regularFile : getParameters().getAdditionalFiles().get()) {
            excludePatterns.addAll(buildExcludePatterns(regularFile.getAsFile()));
        }
    }

    public Set<String> getExcludePatterns() {
        return excludePatterns;
    }

    private Set<String> buildExcludePatterns(File file) {
        List<MutedTest> mutedTests;

        try (InputStream is = new BufferedInputStream(new FileInputStream(file))) {
            mutedTests = objectMapper.readValue(is, MutedTests.class).getTests();
            if (mutedTests == null) {
                return Collections.emptySet();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        Set<String> excludes = new LinkedHashSet<>();
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

                        if (paramString != null) {
                            // Some parameterized tests end up with display names like:
                            // test {yaml=...}
                            //
                            // Gradle 9.4+ treats patterns that include the parameter payload (e.g. "{yaml=...}") as path-based patterns
                            // and fails the build if only class-based tests are selected. To keep mutes working across all execution modes,
                            // we avoid adding the full display-name pattern and instead add:
                            // - a method-only pattern, and
                            // - a method + " *" pattern to cover parameterized display names.
                            excludes.add(mutedTest.getClassName() + "." + methodWithoutParams);
                            excludes.add(mutedTest.getClassName() + "." + methodWithoutParams + " *");
                        } else {
                            // We need to add the following, in case we're skipping an entire class of parameterized tests
                            excludes.add(mutedTest.getClassName() + "." + method + " *");
                            excludes.add(mutedTest.getClassName() + "." + method);
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

        ListProperty<RegularFile> getAdditionalFiles();
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
