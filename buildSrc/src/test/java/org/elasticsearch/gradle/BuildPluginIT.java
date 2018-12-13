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

import org.apache.commons.io.IOUtils;
import org.elasticsearch.gradle.test.GradleIntegrationTestCase;
import org.gradle.testkit.runner.BuildResult;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class BuildPluginIT extends GradleIntegrationTestCase {

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
