/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test;

import org.gradle.api.Project;
import org.gradle.api.provider.Provider;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertThrows;

public class MutedTestsBuildServiceTests {

    private static final String TEST_TASK_PATH = ":test";
    private static final String OTHER_TASK_PATH = ":otherTest";

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private MutedTestsBuildService registerService(File infoDir) {
        return registerService(infoDir, Collections.emptyList());
    }

    private MutedTestsBuildService registerService(File infoDir, List<org.gradle.api.file.RegularFile> additionalFiles) {
        Project project = ProjectBuilder.builder().build();
        Provider<MutedTestsBuildService> provider = project.getGradle()
            .getSharedServices()
            .registerIfAbsent("mutedTests", MutedTestsBuildService.class, spec -> {
                spec.getParameters().getInfoPath().fileValue(infoDir);
                spec.getParameters().getAdditionalFiles().set(additionalFiles);
            });
        return provider.get();
    }

    private void writeMutedTestsYaml(File dir, String yaml) throws IOException {
        Files.write(new File(dir, "muted-tests.yml").toPath(), yaml.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * A single-method mute produces two patterns: the exact method and a wildcard suffix for parameterized runners.
     */
    @Test
    public void testSingleMethodMute() throws IOException {
        File dir = temporaryFolder.newFolder();
        writeMutedTestsYaml(dir, """
            tests:
            - class: org.elasticsearch.SomeTest
              method: testFoo
              issue: https://github.com/elastic/elasticsearch/issues/1
            """);

        Set<String> patterns = registerService(dir).getExcludePatternsForTask(TEST_TASK_PATH);

        assertThat(patterns, hasItems("org.elasticsearch.SomeTest.testFoo", "org.elasticsearch.SomeTest.testFoo *"));
    }

    /**
     * Multiple methods listed under {@code methods} are each expanded to the same pair of patterns.
     */
    @Test
    public void testMultipleMethodsMute() throws IOException {
        File dir = temporaryFolder.newFolder();
        writeMutedTestsYaml(dir, """
            tests:
            - class: org.elasticsearch.SomeTest
              methods:
              - testFoo
              - testBar
              issue: https://github.com/elastic/elasticsearch/issues/1
            """);

        Set<String> patterns = registerService(dir).getExcludePatternsForTask(TEST_TASK_PATH);

        assertThat(
            patterns,
            hasItems(
                "org.elasticsearch.SomeTest.testFoo",
                "org.elasticsearch.SomeTest.testFoo *",
                "org.elasticsearch.SomeTest.testBar",
                "org.elasticsearch.SomeTest.testBar *"
            )
        );
    }

    /**
     * A parameterized method (name contains " {") produces the full name pattern AND the bare method-name pattern,
     * because the randomised runner checks both.
     */
    @Test
    public void testParameterizedMethodMute() throws IOException {
        File dir = temporaryFolder.newFolder();
        writeMutedTestsYaml(dir, """
            tests:
            - class: org.elasticsearch.yaml.SuiteIT
              method: "test {yaml=analysis-common/30_tokenizers/letter}"
              issue: https://github.com/elastic/elasticsearch/issues/2
            """);

        Set<String> patterns = registerService(dir).getExcludePatternsForTask(TEST_TASK_PATH);

        assertThat(
            patterns,
            hasItems(
                "org.elasticsearch.yaml.SuiteIT.test {yaml=analysis-common/30_tokenizers/letter}",
                "org.elasticsearch.yaml.SuiteIT.test"
            )
        );
    }

    /**
     * A class-level mute (no method) produces a single wildcard pattern covering all methods in that class.
     */
    @Test
    public void testClassLevelMute() throws IOException {
        File dir = temporaryFolder.newFolder();
        writeMutedTestsYaml(dir, """
            tests:
            - class: org.elasticsearch.EntireClassTest
              issue: https://github.com/elastic/elasticsearch/issues/3
            """);

        Set<String> patterns = registerService(dir).getExcludePatternsForTask(TEST_TASK_PATH);

        assertThat(patterns, hasItems("org.elasticsearch.EntireClassTest.*"));
    }

    @Test
    public void testTaskScopedMuteAppliesOnlyToMatchingTask() throws IOException {
        File dir = temporaryFolder.newFolder();
        writeMutedTestsYaml(dir, """
            tests:
            - class: org.elasticsearch.SomeTest
              method: testFoo
              tasks:
              - :otherTest
              issue: https://github.com/elastic/elasticsearch/issues/4
            """);

        MutedTestsBuildService service = registerService(dir);

        assertThat(service.getExcludePatternsForTask(TEST_TASK_PATH), is(empty()));
        assertThat(service.getExcludePatternsForTask(OTHER_TASK_PATH), hasItems("org.elasticsearch.SomeTest.testFoo"));
    }

    @Test
    public void testTaskScopedMuteAppliesToEveryListedTask() throws IOException {
        File dir = temporaryFolder.newFolder();
        writeMutedTestsYaml(dir, """
            tests:
            - class: org.elasticsearch.SomeTest
              method: testFoo
              tasks:
              - :otherTest
              - :test
              issue: https://github.com/elastic/elasticsearch/issues/5
            """);

        MutedTestsBuildService service = registerService(dir);

        assertThat(service.getExcludePatternsForTask(TEST_TASK_PATH), hasItems("org.elasticsearch.SomeTest.testFoo"));
        assertThat(service.getExcludePatternsForTask(OTHER_TASK_PATH), hasItems("org.elasticsearch.SomeTest.testFoo"));
    }

    @Test
    public void testTaskScopedClassLevelMute() throws IOException {
        File dir = temporaryFolder.newFolder();
        writeMutedTestsYaml(dir, """
            tests:
            - class: org.elasticsearch.EntireClassTest
              tasks:
              - :test
              issue: https://github.com/elastic/elasticsearch/issues/6
            """);

        Set<String> patterns = registerService(dir).getExcludePatternsForTask(TEST_TASK_PATH);

        assertThat(patterns, contains("org.elasticsearch.EntireClassTest.*"));
    }

    @Test
    public void testTaskScopedParameterizedMethodMute() throws IOException {
        File dir = temporaryFolder.newFolder();
        writeMutedTestsYaml(dir, """
            tests:
            - class: org.elasticsearch.yaml.SuiteIT
              method: "test {yaml=analysis-common/30_tokenizers/letter}"
              tasks:
              - :test
              issue: https://github.com/elastic/elasticsearch/issues/7
            """);

        Set<String> patterns = registerService(dir).getExcludePatternsForTask(TEST_TASK_PATH);

        assertThat(
            patterns,
            contains(
                "org.elasticsearch.yaml.SuiteIT.test",
                "org.elasticsearch.yaml.SuiteIT.test {yaml=analysis-common/30_tokenizers/letter}"
            )
        );
    }

    /**
     * An empty {@code tests} list in the YAML produces no exclude patterns.
     */
    @Test
    public void testEmptyTestsListYieldsNoPatterns() throws IOException {
        File dir = temporaryFolder.newFolder();
        writeMutedTestsYaml(dir, "tests:\n");

        Set<String> patterns = registerService(dir).getExcludePatternsForTask(TEST_TASK_PATH);

        assertThat(patterns, is(empty()));
    }

    /**
     * An additional muted-tests file is merged with the primary file.
     */
    @Test
    public void testAdditionalFileIsMerged() throws IOException {
        File primaryDir = temporaryFolder.newFolder();
        writeMutedTestsYaml(primaryDir, """
            tests:
            - class: org.elasticsearch.PrimaryTest
              method: testPrimary
              issue: https://github.com/elastic/elasticsearch/issues/10
            """);

        File additionalDir = temporaryFolder.newFolder();
        writeMutedTestsYaml(additionalDir, """
            tests:
            - class: org.elasticsearch.AdditionalTest
              method: testAdditional
              issue: https://github.com/elastic/elasticsearch/issues/11
            """);

        Project project = ProjectBuilder.builder().build();
        org.gradle.api.file.RegularFile additionalFile = project.getLayout()
            .getProjectDirectory()
            .file(new File(additionalDir, "muted-tests.yml").getAbsolutePath());

        Set<String> patterns = registerService(primaryDir, List.of(additionalFile)).getExcludePatternsForTask(TEST_TASK_PATH);

        assertThat(
            patterns,
            hasItems(
                "org.elasticsearch.AdditionalTest.testAdditional",
                "org.elasticsearch.PrimaryTest.testPrimary"
            )
        );
    }

    @Test
    public void testDuplicateTaskValuesAreIgnoredAndPatternsStaySorted() throws IOException {
        File dir = temporaryFolder.newFolder();
        writeMutedTestsYaml(dir, """
            tests:
            - class: org.elasticsearch.SomeTest
              methods:
              - testFoo
              - testBar
              tasks:
              - :test
              - :otherTest
              - :test
              issue: https://github.com/elastic/elasticsearch/issues/12
            - class: org.elasticsearch.SomeTest
              method: testBar
              tasks:
              - :test
              issue: https://github.com/elastic/elasticsearch/issues/13
            """);

        List<String> patterns = new ArrayList<>(registerService(dir).getExcludePatternsForTask(TEST_TASK_PATH));

        assertThat(
            patterns,
            contains(
                "org.elasticsearch.SomeTest.testBar",
                "org.elasticsearch.SomeTest.testBar *",
                "org.elasticsearch.SomeTest.testFoo",
                "org.elasticsearch.SomeTest.testFoo *"
            )
        );
    }

    @Test
    public void testEmptyTasksListIsRejected() throws IOException {
        File dir = temporaryFolder.newFolder();
        writeMutedTestsYaml(dir, """
            tests:
            - class: org.elasticsearch.SomeTest
              method: testFoo
              tasks: []
              issue: https://github.com/elastic/elasticsearch/issues/14
            """);

        assertInvalidTasksYaml(dir, "muted test tasks must not be empty");
    }

    @Test
    public void testBlankTaskValueIsRejected() throws IOException {
        File dir = temporaryFolder.newFolder();
        writeMutedTestsYaml(dir, """
            tests:
            - class: org.elasticsearch.SomeTest
              method: testFoo
              tasks:
              - "  "
              issue: https://github.com/elastic/elasticsearch/issues/15
            """);

        assertInvalidTasksYaml(dir, "muted test tasks must not be blank");
    }

    @Test
    public void testNonStringTaskValueIsRejected() throws IOException {
        File dir = temporaryFolder.newFolder();
        writeMutedTestsYaml(dir, """
            tests:
            - class: org.elasticsearch.SomeTest
              method: testFoo
              tasks:
              - 7
              issue: https://github.com/elastic/elasticsearch/issues/16
            """);

        assertInvalidTasksYaml(dir, "muted test tasks must be strings");
    }

    @Test
    public void testTaskValueMustStartWithColon() throws IOException {
        File dir = temporaryFolder.newFolder();
        writeMutedTestsYaml(dir, """
            tests:
            - class: org.elasticsearch.SomeTest
              method: testFoo
              tasks:
              - test
              issue: https://github.com/elastic/elasticsearch/issues/17
            """);

        assertInvalidTasksYaml(dir, "muted test tasks must start with ':'");
    }

    private void assertInvalidTasksYaml(File dir, String expectedMessage) {
        RuntimeException exception = assertThrows(RuntimeException.class, () -> registerService(dir));
        Throwable cause = exception;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        assertThat(cause.getMessage(), containsString(expectedMessage));
    }
}
