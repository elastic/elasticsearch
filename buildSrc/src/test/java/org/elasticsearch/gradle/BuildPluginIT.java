/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
import java.util.Arrays;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class BuildPluginIT extends GradleIntegrationTestCase {

    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    public void testPluginCanBeApplied() {
        BuildResult result = getGradleRunner("elasticsearch.build")
            .withArguments("hello", "-s")
            .build();
        assertTaskSuccessful(result, ":hello");
        assertOutputContains("build plugin can be applied");
    }

    public void testCheckTask() {
        BuildResult result = getGradleRunner("elasticsearch.build")
            .withArguments("check", "assemble", "-s", "-Dlocal.repo.path=" + getLocalTestRepoPath())
            .build();
        assertTaskSuccessful(result, ":check");
    }

    public void testInsecureMavenRepository() throws IOException {
        final String name = "elastic-maven";
        final String url = "http://s3.amazonaws.com/artifacts.elastic.co/maven";
        // add an insecure maven repository to the build.gradle
        final List<String> lines = Arrays.asList(
            "repositories {",
            "  maven {",
            "    name \"elastic-maven\"",
            "    url \"" +  url + "\"\n",
            "  }",
            "}");
        runInsecureArtifactRepositoryTest(name, url, lines);
    }

    public void testInsecureIvyRepository() throws IOException {
        final String name = "elastic-ivy";
        final String url = "http://s3.amazonaws.com/artifacts.elastic.co/ivy";
        // add an insecure ivy repository to the build.gradle
        final List<String> lines = Arrays.asList(
            "repositories {",
            "  ivy {",
            "    name \"elastic-ivy\"",
            "    url \"" +  url + "\"\n",
            "  }",
            "}");
        runInsecureArtifactRepositoryTest(name, url, lines);
    }

    private void runInsecureArtifactRepositoryTest(final String name, final String url, final List<String> lines) throws IOException {
        final File projectDir = getProjectDir("elasticsearch.build");
        FileUtils.copyDirectory(projectDir, tmpDir.getRoot(), pathname -> pathname.getPath().contains("/build/") == false);
        final List<String> buildGradleLines =
            Files.readAllLines(tmpDir.getRoot().toPath().resolve("build.gradle"), StandardCharsets.UTF_8);
        buildGradleLines.addAll(lines);
        Files.write(tmpDir.getRoot().toPath().resolve("build.gradle"), buildGradleLines, StandardCharsets.UTF_8);
        final BuildResult result = GradleRunner.create()
            .withProjectDir(tmpDir.getRoot())
            .withArguments("clean", "hello", "-s", "-i", "--warning-mode=all", "--scan")
            .withPluginClasspath()
            .buildAndFail();
        assertOutputContains(
            result.getOutput(),
            "repository [" + name + "] on project with path [:] is not using a secure protocol for artifacts on [" + url + "]");
    }

    public void testLicenseAndNotice() throws IOException {
        BuildResult result = getGradleRunner("elasticsearch.build")
            .withArguments("clean", "assemble", "-s", "-Dlocal.repo.path=" + getLocalTestRepoPath())
            .build();

        assertTaskSuccessful(result, ":assemble");

        assertBuildFileExists(result, "elasticsearch.build", "distributions/elasticsearch.build.jar");

        try (ZipFile zipFile = new ZipFile(new File(
            getBuildDir("elasticsearch.build"), "distributions/elasticsearch.build.jar"
        ))) {
            ZipEntry licenseEntry = zipFile.getEntry("META-INF/LICENSE.txt");
            ZipEntry noticeEntry = zipFile.getEntry("META-INF/NOTICE.txt");
            assertNotNull("Jar does not have META-INF/LICENSE.txt", licenseEntry);
            assertNotNull("Jar does not have META-INF/NOTICE.txt", noticeEntry);
            try (
                InputStream license = zipFile.getInputStream(licenseEntry);
                InputStream notice = zipFile.getInputStream(noticeEntry)
            ) {
                assertEquals("this is a test license file", IOUtils.toString(license, StandardCharsets.UTF_8.name()));
                assertEquals("this is a test notice file", IOUtils.toString(notice, StandardCharsets.UTF_8.name()));
            }
        }
    }


}
