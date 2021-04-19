/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.gradle.test.GradleIntegrationTestCase;
import org.gradle.testkit.runner.BuildResult;
import org.gradle.testkit.runner.GradleRunner;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.elasticsearch.gradle.test.TestClasspathUtils.setupJarJdkClasspath;

public class BuildPluginIT extends GradleIntegrationTestCase {

    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    public void testPluginCanBeApplied() {
        BuildResult result = getGradleRunner("elasticsearch.build").withArguments("hello", "-s").build();
        assertTaskSuccessful(result, ":hello");
        assertOutputContains("build plugin can be applied");
    }

    public void testCheckTask() {
        setupJarJdkClasspath(getProjectDir("elasticsearch.build"));
        BuildResult result = getGradleRunner("elasticsearch.build").withArguments("check", "assemble", "-s").build();
        assertTaskSuccessful(result, ":check");
    }

    private void runInsecureArtifactRepositoryTest(final String name, final String url, final List<String> lines) throws IOException {
        final File projectDir = getProjectDir("elasticsearch.build");
        final Path projectDirPath = projectDir.toPath();
        FileUtils.copyDirectory(projectDir, tmpDir.getRoot(), file -> {
            final Path relativePath = projectDirPath.relativize(file.toPath());
            for (Path segment : relativePath) {
                if (segment.toString().equals("build")) {
                    return false;
                }
            }
            return true;
        });
        final List<String> buildGradleLines = Files.readAllLines(tmpDir.getRoot().toPath().resolve("build.gradle"), StandardCharsets.UTF_8);
        buildGradleLines.addAll(lines);
        Files.write(tmpDir.getRoot().toPath().resolve("build.gradle"), buildGradleLines, StandardCharsets.UTF_8);
        final BuildResult result = GradleRunner.create()
            .withProjectDir(tmpDir.getRoot())
            .withArguments("clean", "hello", "-s", "-i", "--warning-mode=all", "--scan")
            .withPluginClasspath()
            .forwardOutput()
            .buildAndFail();

        assertOutputContains(
            result.getOutput(),
            "repository [" + name + "] on project with path [:] is not using a secure protocol for artifacts on [" + url + "]"
        );
    }

    public void testLicenseAndNotice() throws IOException {
        BuildResult result = getGradleRunner("elasticsearch.build").withArguments("clean", "assemble").build();

        assertTaskSuccessful(result, ":assemble");

        assertBuildFileExists(result, "elasticsearch.build", "distributions/elasticsearch.build.jar");

        try (ZipFile zipFile = new ZipFile(new File(getBuildDir("elasticsearch.build"), "distributions/elasticsearch.build.jar"))) {
            ZipEntry licenseEntry = zipFile.getEntry("META-INF/LICENSE.txt");
            ZipEntry noticeEntry = zipFile.getEntry("META-INF/NOTICE.txt");
            assertNotNull("Jar does not have META-INF/LICENSE.txt", licenseEntry);
            assertNotNull("Jar does not have META-INF/NOTICE.txt", noticeEntry);
            try (InputStream license = zipFile.getInputStream(licenseEntry); InputStream notice = zipFile.getInputStream(noticeEntry)) {
                assertEquals("this is a test license file", IOUtils.toString(license, StandardCharsets.UTF_8.name()));
                assertEquals("this is a test notice file", IOUtils.toString(notice, StandardCharsets.UTF_8.name()));
            }
        }
    }

}
