/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal.nativelibs;

import org.elasticsearch.gradle.Architecture;
import org.elasticsearch.gradle.OS;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class BuildNativeLibraryTaskTests {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private Project project;
    private BuildNativeLibraryTask task;

    @Before
    public void setUp() {
        project = ProjectBuilder.builder().build();
        task = project.getTasks().create("buildNativeLibs", BuildNativeLibraryTask.class);
    }

    @Test
    public void testGradleGeneratesManagedProperties() {
        assertNotNull(task.getSourceFiles());
        assertNotNull(task.getNativeDir());
        assertNotNull(task.getMode());
        assertNotNull(task.getToolchainImage());
        assertNotNull(task.getOutputDir());
        assertFalse(task.getMode().isPresent());
    }

    /**
     * The platform name decides which directory must be populated for a build to count as successful,
     * and has to match the layout the distribution and test JVMs read, so pin its vocabulary.
     * Deliberately not asserting a specific platform: these tests run on macOS and on both Linux
     * architectures.
     */
    @Test
    public void testHostPlatformShapeAndConsistency() {
        String platform = BuildNativeLibraryTask.hostPlatform();
        assertTrue("unexpected host platform: " + platform, platform.matches("^(darwin|linux|windows)-(x64|aarch64)$"));
        assertEquals(OS.current().javaOsReference + "-" + Architecture.current().javaClassifier, platform);
    }

    @Test
    public void testInvalidModeThrows() {
        task.getMode().set("invalid");
        task.getNativeDir().set(temporaryFolder.getRoot());
        task.getOutputDir().set(new File(temporaryFolder.getRoot(), "output"));

        GradleException ex = assertThrows(GradleException.class, task::build);
        assertTrue(ex.getMessage().contains("Unknown mode: 'invalid'"));
    }

    @Test
    public void testCollectOutputAppliesDeclaredMapping() throws IOException {
        File nativeDir = temporaryFolder.newFolder("native");
        File outputDir = temporaryFolder.newFolder("output");

        Path buildDir = nativeDir.toPath().resolve("out/shared");
        Files.createDirectories(buildDir.resolve("aarch64"));
        Files.createDirectories(buildDir.resolve("amd64"));
        Files.writeString(buildDir.resolve("aarch64/libfoo.dylib"), "darwin-binary");
        Files.writeString(buildDir.resolve("aarch64/libfoo.so"), "linux-arm-binary");
        Files.writeString(buildDir.resolve("amd64/libfoo.so"), "linux-x64-binary");

        task.getCollect().put("out/shared/aarch64/libfoo.dylib", "darwin-aarch64/libfoo.dylib");
        task.getCollect().put("out/shared/aarch64/libfoo.so", "linux-aarch64/libfoo.so");
        task.getCollect().put("out/shared/amd64/libfoo.so", "linux-x64/libfoo.so");

        task.collectOutput(nativeDir, outputDir);

        assertEquals("darwin-binary", Files.readString(outputDir.toPath().resolve("darwin-aarch64/libfoo.dylib")));
        assertEquals("linux-arm-binary", Files.readString(outputDir.toPath().resolve("linux-aarch64/libfoo.so")));
        assertEquals("linux-x64-binary", Files.readString(outputDir.toPath().resolve("linux-x64/libfoo.so")));
    }

    @Test
    public void testCollectOutputCopiesNothingWhenNothingDeclared() throws IOException {
        File nativeDir = temporaryFolder.newFolder("native");
        File outputDir = temporaryFolder.newFolder("output");

        task.collectOutput(nativeDir, outputDir);

        try (var entries = Files.list(outputDir.toPath())) {
            assertTrue("expected an untouched output directory", entries.findAny().isEmpty());
        }
    }

    @Test
    public void testCollectOutputThrowsOnMissingSource() throws IOException {
        File nativeDir = temporaryFolder.newFolder("native");
        File outputDir = temporaryFolder.newFolder("output");

        task.getCollect().put("out/shared/aarch64/libfoo.dylib", "darwin-aarch64/libfoo.dylib");

        GradleException ex = assertThrows(GradleException.class, () -> task.collectOutput(nativeDir, outputDir));
        assertTrue(ex.getMessage().contains("Expected build output not found"));
    }

    /**
     * An unconfigured platform set must not silently verify nothing: with no expected platforms a
     * build that produced no library at all would be accepted, and the missing file would surface far
     * later as a link error.
     */
    @Test
    public void testVerifyOutputRequiresAtLeastOneExpectedPlatform() throws IOException {
        File outputDir = temporaryFolder.newFolder("output");

        GradleException ex = assertThrows(GradleException.class, () -> BuildNativeLibraryTask.verifyOutput(outputDir, Set.of()));
        assertTrue(ex.getMessage().contains("supportedPlatforms"));
    }

    @Test
    public void testVerifyOutputThrowsWhenNothingProduced() throws IOException {
        File outputDir = temporaryFolder.newFolder("output");

        GradleException ex = assertThrows(
            GradleException.class,
            () -> BuildNativeLibraryTask.verifyOutput(outputDir, Set.of("darwin-aarch64", "linux-x64"))
        );
        assertTrue(ex.getMessage().contains("darwin-aarch64"));
        assertTrue(ex.getMessage().contains("linux-x64"));
        assertTrue(ex.getMessage().contains("<empty>"));
    }

    @Test
    public void testVerifyOutputNamesOnlyTheMissingPlatforms() throws IOException {
        File outputDir = temporaryFolder.newFolder("output");
        Path produced = outputDir.toPath().resolve("linux-x64/libfoo.so");
        Files.createDirectories(produced.getParent());
        Files.writeString(produced, "binary");

        GradleException ex = assertThrows(
            GradleException.class,
            () -> BuildNativeLibraryTask.verifyOutput(outputDir, Set.of("darwin-aarch64", "linux-x64"))
        );
        assertTrue(ex.getMessage().contains("darwin-aarch64"));
        assertFalse("the platform that was produced should not be reported missing", ex.getMessage().contains("[linux-x64"));
    }

    @Test
    public void testVerifyOutputPassesWhenEveryExpectedPlatformPopulated() throws IOException {
        File outputDir = temporaryFolder.newFolder("output");
        for (String platform : Set.of("darwin-aarch64", "linux-aarch64", "linux-x64")) {
            Path produced = outputDir.toPath().resolve(platform).resolve("libfoo.so");
            Files.createDirectories(produced.getParent());
            Files.writeString(produced, "binary");
        }

        BuildNativeLibraryTask.verifyOutput(outputDir, Set.of("darwin-aarch64", "linux-aarch64", "linux-x64"));
    }

    /**
     * The reason verification takes its expected platforms as an argument: a container build
     * cross-compiles every platform, so it must verify identically wherever it runs, including on a
     * host the library is never built for, such as Windows or an Intel Mac. Deliberately expects a set
     * that excludes this machine's own platform.
     */
    @Test
    public void testVerifyOutputDoesNotDependOnTheHostPlatform() throws IOException {
        File outputDir = temporaryFolder.newFolder("output");
        Set<String> expected = Set.of("some-other-os-x64", "another-os-aarch64");
        assertFalse("fixture must not accidentally match the host", expected.contains(BuildNativeLibraryTask.hostPlatform()));

        for (String platform : expected) {
            Path produced = outputDir.toPath().resolve(platform).resolve("libfoo.so");
            Files.createDirectories(produced.getParent());
            Files.writeString(produced, "binary");
        }

        BuildNativeLibraryTask.verifyOutput(outputDir, expected);
    }

    @Test
    public void testCopyBuildOutputHandlesExistingTarget() throws IOException {
        File srcDir = temporaryFolder.newFolder("src");
        File destDir = temporaryFolder.newFolder("dest");

        Path source = srcDir.toPath().resolve("lib.so");
        Path dest = destDir.toPath().resolve("platform/lib.so");
        Files.writeString(source, "binary-content");

        BuildNativeLibraryTask.copyBuildOutput(source, dest);

        assertTrue(Files.exists(dest));
        assertEquals("binary-content", Files.readString(dest));

        // Overwrite with new content
        Files.writeString(source, "updated-binary");
        BuildNativeLibraryTask.copyBuildOutput(source, dest);
        assertEquals("updated-binary", Files.readString(dest));
    }
}
