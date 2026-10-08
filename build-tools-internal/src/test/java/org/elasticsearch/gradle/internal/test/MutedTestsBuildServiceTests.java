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
import java.util.Collections;
import java.util.Set;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;

public class MutedTestsBuildServiceTests {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    private MutedTestsBuildService registerService(File infoDir) {
        return registerService(infoDir, Collections.emptyList());
    }

    private MutedTestsBuildService registerService(File infoDir, java.util.List<org.gradle.api.file.RegularFile> additionalFiles) {
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

    // ---------------------------------------------------------------------------
    // Tests
    // ---------------------------------------------------------------------------

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

        Set<String> patterns = registerService(dir).getExcludePatterns();

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

        Set<String> patterns = registerService(dir).getExcludePatterns();

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

        Set<String> patterns = registerService(dir).getExcludePatterns();

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

        Set<String> patterns = registerService(dir).getExcludePatterns();

        assertThat(patterns, hasItems("org.elasticsearch.EntireClassTest.*"));
    }

    /**
     * An empty {@code tests} list in the YAML produces no exclude patterns.
     */
    @Test
    public void testEmptyTestsListYieldsNoPatterns() throws IOException {
        File dir = temporaryFolder.newFolder();
        writeMutedTestsYaml(dir, "tests:\n");

        Set<String> patterns = registerService(dir).getExcludePatterns();

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

        Set<String> patterns = registerService(primaryDir, java.util.List.of(additionalFile)).getExcludePatterns();

        assertThat(
            patterns,
            hasItems(
                "org.elasticsearch.PrimaryTest.testPrimary",
                "org.elasticsearch.AdditionalTest.testAdditional"
            )
        );
    }
}
