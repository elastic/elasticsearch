/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.test.rest.yaml;

import org.elasticsearch.test.ESTestCase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.greaterThan;

public class ESClientYamlSuiteTestCaseTests extends ESTestCase {

    public void testLoadAllYamlSuites() throws Exception {
        Map<String, Set<Path>> yamlSuites = ESClientYamlSuiteTestCase.loadSuites("");
        assertEquals(2, yamlSuites.size());
    }

    public void testLoadSingleYamlSuite() throws Exception {
        Map<String, Set<Path>> yamlSuites = ESClientYamlSuiteTestCase.loadSuites("suite1/10_basic");
        assertSingleFile(yamlSuites, "suite1", "10_basic.yml");

        // extension .yaml is optional
        yamlSuites = ESClientYamlSuiteTestCase.loadSuites("suite1/10_basic");
        assertSingleFile(yamlSuites, "suite1", "10_basic.yml");
    }

    public void testLoadMultipleYamlSuites() throws Exception {
        // single directory
        Map<String, Set<Path>> yamlSuites = ESClientYamlSuiteTestCase.loadSuites("suite1");
        assertThat(yamlSuites, notNullValue());
        assertThat(yamlSuites.size(), equalTo(1));
        assertThat(yamlSuites.containsKey("suite1"), equalTo(true));
        assertThat(yamlSuites.get("suite1").size(), greaterThan(1));

        // multiple directories
        yamlSuites = ESClientYamlSuiteTestCase.loadSuites("suite1", "suite2");
        assertThat(yamlSuites, notNullValue());
        assertThat(yamlSuites.size(), equalTo(2));
        assertThat(yamlSuites.containsKey("suite1"), equalTo(true));
        assertEquals(2, yamlSuites.get("suite1").size());
        assertThat(yamlSuites.containsKey("suite2"), equalTo(true));
        assertEquals(2, yamlSuites.get("suite2").size());

        // multiple paths, which can be both directories or yaml test suites (with optional file extension)
        yamlSuites = ESClientYamlSuiteTestCase.loadSuites("suite2/10_basic", "suite1");
        assertThat(yamlSuites, notNullValue());
        assertThat(yamlSuites.size(), equalTo(2));
        assertThat(yamlSuites.containsKey("suite2"), equalTo(true));
        assertThat(yamlSuites.get("suite2").size(), equalTo(1));
        assertSingleFile(yamlSuites.get("suite2"), "suite2", "10_basic.yml");
        assertThat(yamlSuites.containsKey("suite1"), equalTo(true));
        assertThat(yamlSuites.get("suite1").size(), greaterThan(1));

        // files can be loaded from classpath and from file system too
        Path dir = createTempDir();
        Path file = dir.resolve("test_loading.yml");
        Files.createFile(file);
    }

    /**
     * A yamlRestTest task can expose more than one {@code rest-api-spec/test} root on its
     * classpath (e.g. its own source set output and {@code copyYamlTestsTask} output). A
     * user-specified suite path typically lives in only one of those roots, so
     * {@link ESClientYamlSuiteTestCase#loadSuites(Path[], String...)} must not require the
     * path to be present in every root - existence in at least one root is enough. This
     * regression reproduces the {@code AssertionError: ... does not exist in YAML test root}
     * seen by {@code repeat-changed-tests} once {@code tests.rest.suite.<task>} routing
     * was wired up.
     */
    public void testLoadSingleSuiteAcrossMultipleRoots() throws Exception {
        // given: two roots, only one of which contains the requested suite. The other root has
        // unrelated content so it is non-empty (mirrors copyYamlTestsTask output in real builds).
        Path rootWithSuite = createTempDir();
        Path suiteDir = rootWithSuite.resolve("logging");
        Files.createDirectories(suiteDir);
        Files.writeString(suiteDir.resolve("10_usage.yml"), "---\n\"placeholder\":\n  - do: { ping: {} }\n");

        Path rootWithoutSuite = createTempDir();
        Path otherDir = rootWithoutSuite.resolve("other");
        Files.createDirectories(otherDir);
        Files.writeString(otherDir.resolve("10_other.yml"), "---\n\"placeholder\":\n  - do: { ping: {} }\n");

        // when: loading the suite from the multi-root classpath
        Map<String, Set<Path>> yamlSuites = ESClientYamlSuiteTestCase.loadSuites(
            new Path[] { rootWithSuite, rootWithoutSuite },
            "logging/10_usage"
        );

        // then: the suite is found despite being absent from the second root
        assertSingleFile(yamlSuites, "logging", "10_usage.yml");

        // when: roots are passed in the reverse order
        yamlSuites = ESClientYamlSuiteTestCase.loadSuites(new Path[] { rootWithoutSuite, rootWithSuite }, "logging/10_usage");

        // then: the result is identical; root order must not matter
        assertSingleFile(yamlSuites, "logging", "10_usage.yml");
    }

    /**
     * If a suite path resolves under more than one root (e.g. the project's own source set output
     * and the {@code copyYamlTestsTask} output both contain a same-named file), the loader registers
     * both {@link Path}s under the same group key. Downstream the suite is parsed and executed once
     * per copy, and {@code addSuite} emits a "duplicate test name" warning. This documents the
     * status quo - the multi-root tolerance added for the missing-from-one-root case does not
     * change how same-named copies are handled.
     */
    public void testLoadSingleSuitePresentInBothRoots() throws Exception {
        // given: the same suite file exists under both roots
        Path rootA = createTempDir();
        Path rootB = createTempDir();
        for (Path root : new Path[] { rootA, rootB }) {
            Path suiteDir = root.resolve("logging");
            Files.createDirectories(suiteDir);
            Files.writeString(suiteDir.resolve("10_usage.yml"), "---\n\"placeholder\":\n  - do: { ping: {} }\n");
        }

        // when: loading the suite
        Map<String, Set<Path>> yamlSuites = ESClientYamlSuiteTestCase.loadSuites(new Path[] { rootA, rootB }, "logging/10_usage");

        // then: both absolute paths are registered under the same group, so downstream the suite
        // will be parsed and executed once per copy (and addSuite logs a duplicate-name warning)
        assertThat(yamlSuites, notNullValue());
        assertThat(yamlSuites.keySet(), equalTo(Set.of("logging")));
        Set<Path> paths = yamlSuites.get("logging");
        assertThat("both root copies should be registered", paths.size(), equalTo(2));
        assertThat(paths, equalTo(Set.of(rootA.resolve("logging/10_usage.yml"), rootB.resolve("logging/10_usage.yml"))));
    }

    /**
     * Projects whose yamlRestCompatTest task has no compat tests at all (e.g. {@code modules/user-agent})
     * expose zero {@code rest-api-spec/test} roots on the test JVM classpath, but the test class still
     * calls {@code createParameters()} with the default empty path. That must produce an empty parameter
     * set, not an {@code AssertionError} - JUnit Parameterized treats it as zero candidates and the test
     * class becomes a no-op. The typo guard must not fire for the "include everything" sentinel.
     */
    public void testLoadEmptyPathWithNoRootsReturnsEmpty() throws Exception {
        // given: no classpath roots at all (mirrors a project with no yaml tests)
        Path[] roots = new Path[0];

        // when: loading with the default "include everything" sentinel
        Map<String, Set<Path>> yamlSuites = ESClientYamlSuiteTestCase.loadSuites(roots, "");

        // then: an empty result is returned without firing the typo guard
        assertThat(yamlSuites, notNullValue());
        assertThat(yamlSuites.isEmpty(), equalTo(true));
    }

    public void testLoadMissingSuiteFailsWithDescriptiveError() throws Exception {
        // given: a single empty root, so the requested suite cannot resolve anywhere
        Path root = createTempDir();

        // when: loading a suite path that exists in none of the roots
        AssertionError error = expectThrows(
            AssertionError.class,
            () -> ESClientYamlSuiteTestCase.loadSuites(new Path[] { root }, "missing/never_exists")
        );

        // then: the typo-guard assertion fires and names both the missing path and the roots searched
        assertThat(error.getMessage(), containsString("missing/never_exists"));
        assertThat(error.getMessage(), containsString("does not exist in any YAML test root"));
    }

    private static void assertSingleFile(Map<String, Set<Path>> yamlSuites, String dirName, String fileName) {
        assertThat(yamlSuites, notNullValue());
        assertThat(yamlSuites.size(), equalTo(1));
        assertThat(yamlSuites.containsKey(dirName), equalTo(true));
        assertSingleFile(yamlSuites.get(dirName), dirName, fileName);
    }

    private static void assertSingleFile(Set<Path> files, String dirName, String fileName) {
        assertThat(files.size(), equalTo(1));
        Path file = files.iterator().next();
        assertThat(file.getFileName().toString(), equalTo(fileName));
        assertThat(file.toAbsolutePath().getParent().getFileName().toString(), equalTo(dirName));
    }
}
