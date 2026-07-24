/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.EnumSet;

import static org.hamcrest.Matchers.containsString;

/**
 * Verifies that existing-resource aliases reject accidental mutations, preserve normal reads, permit controlled overwrite,
 * and leave default-path creation unchanged.
 */
public class GoldenExistingPathGoldenTests extends GoldenTestCase {
    private static final String EXISTING_QUERY = "ROW a = 1\n";
    private String previousOverwriteProperty;

    @Override
    @SuppressForbidden(reason = "Tests must isolate the golden.overwrite system property")
    public void setUp() throws Exception {
        super.setUp();
        previousOverwriteProperty = System.clearProperty("golden.overwrite");
    }

    @Override
    public void tearDown() throws Exception {
        try {
            restoreOverwriteProperty(previousOverwriteProperty);
        } finally {
            super.tearDown();
        }
    }

    public void testRejectsMismatchedQuery() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> builder("ROW a = 2\n").existingGoldenPath("GoldenExistingPathGoldenTests", "existing").run()
        );
        assertThat(e.getMessage(), containsString("GoldenExistingPathGoldenTests#testRejectsMismatchedQuery"));
        assertThat(e.getMessage(), containsString(PathUtils.get("existing", "query.esql").toString()));
    }

    public void testRejectsMissingExpectedFile() {
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> builder(EXISTING_QUERY).stages(EnumSet.of(Stage.LOGICAL_OPTIMIZATION))
                .existingGoldenPath("GoldenExistingPathGoldenTests", "existing")
                .run()
        );
        assertThat(e.getMessage(), containsString("GoldenExistingPathGoldenTests#testRejectsMissingExpectedFile"));
        assertThat(e.getMessage(), containsString("logical_optimization.expected"));
    }

    public void testRejectsPathWithoutExpectedFiles() {
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> builder(EXISTING_QUERY).existingGoldenPath("GoldenExistingPathGoldenTests", "existing")
                .nestedPath("without_expected")
                .run()
        );
        assertThat(e.getMessage(), containsString("GoldenExistingPathGoldenTests#testRejectsPathWithoutExpectedFiles"));
        assertThat(e.getMessage(), containsString("has no expected files"));
    }

    public void testRejectsNestedTraversal() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> builder(EXISTING_QUERY).existingGoldenPath("GoldenExistingPathGoldenTests", "existing").nestedPath("..").run()
        );
        assertThat(e.getMessage(), containsString("invalid golden nested path component [..]"));
    }

    public void testReadsExistingPathWithoutWritingQuery() throws Exception {
        String nestedPath = uniqueNestedPath("read");
        Path existingRoot = goldenTestsRoot().resolve("GoldenExistingPathGoldenTests/existing");
        Path testDir = existingRoot.resolve(nestedPath);
        Path queryPath = testDir.resolve("query.esql");
        try {
            Files.createDirectory(testDir);
            Files.writeString(queryPath, EXISTING_QUERY);
            Files.copy(existingRoot.resolve("overwrite/analysis.expected"), testDir.resolve("analysis.expected"));
            Files.setLastModifiedTime(queryPath, FileTime.fromMillis(1_000_000_000_000L));
            FileTime markedModifiedTime = Files.getLastModifiedTime(queryPath);

            builder(EXISTING_QUERY).stages(EnumSet.of(Stage.ANALYSIS))
                .existingGoldenPath("GoldenExistingPathGoldenTests", "existing")
                .nestedPath(nestedPath)
                .run();

            assertEquals(markedModifiedTime, Files.getLastModifiedTime(queryPath));
        } finally {
            deletePaths(testDir.resolve("analysis.expected"), queryPath, testDir);
        }
    }

    public void testCreatesDefaultPathOnFirstRun() throws Exception {
        Path methodRoot = goldenTestsRoot().resolve("GoldenExistingPathGoldenTests/testCreatesDefaultPathOnFirstRun");
        Path testRoot = methodRoot.resolve(uniqueNestedPath("default"));
        try {
            assertFalse(Files.exists(testRoot));

            builder(EXISTING_QUERY).stages(EnumSet.of(Stage.ANALYSIS)).nestedPath(testRoot.getFileName().toString()).run();

            assertTrue(Files.isRegularFile(testRoot.resolve("query.esql")));
            assertTrue(Files.isRegularFile(testRoot.resolve("analysis.expected")));
        } finally {
            deletePaths(testRoot.resolve("analysis.expected"), testRoot.resolve("query.esql"), testRoot);
        }
    }

    @SuppressForbidden(reason = "Overwrite behavior is controlled by the golden.overwrite system property")
    public void testOverwriteExistingPathCreatesDeclaredRange() throws Exception {
        String nestedPath = uniqueNestedPath("overwrite");
        Path testDir = goldenTestsRoot().resolve("GoldenExistingPathGoldenTests/existing").resolve(nestedPath);
        Path rangeDir = testDir.resolve("before_compact_multi_type_es_field");
        try {
            Files.createDirectory(testDir);
            Files.writeString(testDir.resolve("query.esql"), EXISTING_QUERY);
            Files.writeString(testDir.resolve("analysis.expected"), "stale\n");
            System.setProperty("golden.overwrite", "true");

            builder(EXISTING_QUERY).stages(EnumSet.of(Stage.ANALYSIS))
                .existingGoldenPath("GoldenExistingPathGoldenTests", "existing")
                .nestedPath(nestedPath)
                .expectationChangesAt("compact_multi_type_es_field")
                .run();

            assertNotEquals("stale\n", Files.readString(testDir.resolve("analysis.expected")));
            assertTrue(Files.isRegularFile(rangeDir.resolve("analysis.expected")));
            assertFalse(Files.exists(rangeDir.resolve("query.esql")));
        } finally {
            deletePaths(
                rangeDir.resolve("analysis.expected"),
                rangeDir,
                testDir.resolve("analysis.expected"),
                testDir.resolve("query.esql"),
                testDir
            );
        }
    }

    private String uniqueNestedPath(String prefix) {
        return prefix + "_" + ProcessHandle.current().pid() + "_" + randomAlphaOfLength(10);
    }

    private static void deletePaths(Path... paths) throws IOException {
        IOException failure = null;
        for (Path path : paths) {
            try {
                Files.deleteIfExists(path);
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    @SuppressForbidden(reason = "Tests must restore the golden.overwrite system property")
    private static void restoreOverwriteProperty(String previous) {
        if (previous == null) {
            System.clearProperty("golden.overwrite");
        } else {
            System.setProperty("golden.overwrite", previous);
        }
    }
}
