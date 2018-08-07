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
import org.elasticsearch.gradle.test.GradleIntegrationTestCase;
import org.gradle.testkit.runner.GradleRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class BuildExamplePluginsIT extends GradleIntegrationTestCase {

    private static List<File> EXTERNAL_PROJECTS = Arrays.stream(
        Objects.requireNonNull(System.getProperty("test.build-tools.plugin.examples"))
            .split(File.pathSeparator)
    ).map(File::new).collect(Collectors.toList());

    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    @BeforeClass
    public static void assertProjectsExist() {
        List<File> existing = EXTERNAL_PROJECTS.stream().filter(File::exists).collect(Collectors.toList());
        assertEquals(EXTERNAL_PROJECTS, existing);
    }

    @AfterClass
    public static void assertEverythingTested() {
        assertEquals(
            "Some example plugins are not tested: " + EXTERNAL_PROJECTS,
            0, EXTERNAL_PROJECTS.size()
        );
    }

    public void testCustomSettings() throws IOException {
        runAndRemove("custom-settings");
    }

    public void testPainlessWhitelist() throws IOException {
        runAndRemove("painless-whitelist");
    }

    public void testRescore() throws IOException {
        runAndRemove("rescore");
    }

    public void testRestHandler() throws IOException {
        runAndRemove("rest-handler");
    }

    public void testScriptExpertScoring() throws IOException {
        runAndRemove("script-expert-scoring");
    }

    private void runAndRemove(String name) throws IOException {
        List<File> matches = EXTERNAL_PROJECTS.stream().filter(each -> each.getAbsolutePath().contains(name))
            .collect(Collectors.toList());
        assertEquals(
            "Expected a single project folder to match `" + name + "` but got: " + matches,
            1, matches.size()
        );

        EXTERNAL_PROJECTS.remove(matches.get(0));

        FileUtils.copyDirectory(matches.get(0), tmpDir.getRoot());
        // just get rid of deprecation warnings from now
        Files.write(
            tmpDir.newFile("settings.gradle").toPath(), "enableFeaturePreview('STABLE_PUBLISHING')\n".getBytes(StandardCharsets.UTF_8)
        );
        // Add a repositories section to be able to resolve from snapshots.
        // !NOTE! that the plugin build will use be using stale artifacts, not the ones produced in the current build
        // TODO: Add "http://s3.amazonaws.com/download.elasticsearch.org/lucenesnapshots/${revision}" 
        //       (revision = (luceneVersion =~ /\w+-snapshot-([a-z0-9]+)/)[0][1])
        Files.write(
            new File(tmpDir.getRoot(), "build.gradle").toPath(),
            ("\n" +
                "repositories {\n" +
                "  maven {\n" +
                "    url \"https://snapshots.elastic.co/maven\"\n" +
                "  }\n" +
                "  maven {\n" +
                "    url \"http://s3.amazonaws.com/download.elasticsearch.org/lucenesnapshots/608f0277b0\"\n" + // FIXME
                "  }\n" +
                "  mavenLocal()\n" + // FIXME
                "}\n").getBytes(StandardCharsets.UTF_8),
            StandardOpenOption.APPEND
        );
        Files.write(
            tmpDir.newFile("NOTICE.txt").toPath(),
            "dummy test nottice".getBytes(StandardCharsets.UTF_8)
        );

        GradleRunner.create()
            // Todo make a copy, write a settings file enabling stable publishing
            .withProjectDir(tmpDir.getRoot())
            .withArguments("clean", "check", "-s", "-i", "--warning-mode=all", "--scan")
            .withPluginClasspath()
            .build();
    }

}
