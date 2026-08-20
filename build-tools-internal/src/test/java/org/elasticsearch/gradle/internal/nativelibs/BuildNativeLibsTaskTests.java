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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class BuildNativeLibsTaskTests {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private Project project;
    private BuildNativeLibsTask task;

    @Before
    public void setUp() {
        project = ProjectBuilder.builder().build();
        task = project.getTasks().create("buildNativeLibs", BuildNativeLibsTask.class);
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
        String platform = BuildNativeLibsTask.hostPlatform();
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

    @Test
    public void testVerifyOutputThrowsWhenNothingProduced() throws IOException {
        File outputDir = temporaryFolder.newFolder("output");

        GradleException ex = assertThrows(GradleException.class, () -> BuildNativeLibsTask.verifyOutput(outputDir));
        assertTrue(ex.getMessage().contains(BuildNativeLibsTask.hostPlatform()));
        assertTrue(ex.getMessage().contains("<empty>"));
    }

    @Test
    public void testVerifyOutputThrowsWhenOnlyOtherPlatformsProduced() throws IOException {
        File outputDir = temporaryFolder.newFolder("output");
        Path wrongPlace = outputDir.toPath().resolve("some-other-platform/libfoo.so");
        Files.createDirectories(wrongPlace.getParent());
        Files.writeString(wrongPlace, "binary");

        GradleException ex = assertThrows(GradleException.class, () -> BuildNativeLibsTask.verifyOutput(outputDir));
        assertTrue(ex.getMessage().contains("some-other-platform/libfoo.so"));
    }

    @Test
    public void testVerifyOutputPassesWhenHostPlatformPopulated() throws IOException {
        File outputDir = temporaryFolder.newFolder("output");
        Path produced = outputDir.toPath().resolve(BuildNativeLibsTask.hostPlatform()).resolve("libfoo.so");
        Files.createDirectories(produced.getParent());
        Files.writeString(produced, "binary");

        BuildNativeLibsTask.verifyOutput(outputDir);
    }

    @Test
    public void testCopyBuildOutputHandlesExistingTarget() throws IOException {
        File srcDir = temporaryFolder.newFolder("src");
        File destDir = temporaryFolder.newFolder("dest");

        Path source = srcDir.toPath().resolve("lib.so");
        Path dest = destDir.toPath().resolve("platform/lib.so");
        Files.writeString(source, "binary-content");

        BuildNativeLibsTask.copyBuildOutput(source, dest);

        assertTrue(Files.exists(dest));
        assertEquals("binary-content", Files.readString(dest));

        // Overwrite with new content
        Files.writeString(source, "updated-binary");
        BuildNativeLibsTask.copyBuildOutput(source, dest);
        assertEquals("updated-binary", Files.readString(dest));
    }
}
