/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle.internal;

import org.apache.commons.io.IOUtils;
import org.elasticsearch.gradle.internal.test.GradleIntegrationTestCase;
import org.gradle.testkit.runner.BuildResult;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.elasticsearch.gradle.internal.test.TestClasspathUtils.setupJarJdkClasspath;

public class BuildPluginIT extends GradleIntegrationTestCase {

    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    @Override
    public String projectName() {
        return "elasticsearch.build";
    }

    public void testPluginCanBeApplied() {
        BuildResult result = getGradleRunner().withArguments("hello", "-s").build();
        assertTaskSuccessful(result, ":hello");
        assertOutputContains("build plugin can be applied");
    }

    public void testCheckTask() {
        setupJarJdkClasspath(getProjectDir());
        BuildResult result = getGradleRunner().withArguments("check", "assemble", "-s").build();
        assertTaskSuccessful(result, ":check");
    }

    public void testLicenseAndNotice() throws IOException {
        BuildResult result = getGradleRunner().withArguments("clean", "assemble").build();
        assertTaskSuccessful(result, ":assemble");
        String expectedFilePath = "distributions/elasticsearch.build.jar";
        assertBuildFileExists(result, projectName(), expectedFilePath);
        try (ZipFile zipFile = new ZipFile(new File(getBuildDir(projectName()), expectedFilePath))) {
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
