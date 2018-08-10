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

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.apache.commons.io.FileUtils;
import org.elasticsearch.gradle.test.GradleIntegrationTestCase;
import org.gradle.testkit.runner.GradleRunner;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class BuildExamplePluginsIT extends GradleIntegrationTestCase {

    private static List<File> EXAMPLE_PLUGINS = Collections.unmodifiableList(
        Arrays.stream(
            Objects.requireNonNull(System.getProperty("test.build-tools.plugin.examples"))
                .split(File.pathSeparator)
        ).map(File::new).collect(Collectors.toList())
    );

    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    public final File examplePlugin;

    public BuildExamplePluginsIT(File examplePlugin) {
        this.examplePlugin = examplePlugin;
    }

    @BeforeClass
    public static void assertProjectsExist() {
        assertEquals(
            EXAMPLE_PLUGINS,
            EXAMPLE_PLUGINS.stream().filter(File::exists).collect(Collectors.toList())
        );
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return EXAMPLE_PLUGINS
            .stream()
            .map(each -> new Object[] {each})
            .collect(Collectors.toList());
    }

    public void testCurrentExamplePlugin() throws IOException {
        FileUtils.copyDirectory(examplePlugin, tmpDir.getRoot());
        // just get rid of deprecation warnings from now
        Files.write(
            tmpDir.newFile("settings.gradle").toPath(), "enableFeaturePreview('STABLE_PUBLISHING')\n".getBytes(StandardCharsets.UTF_8)
        );
        // Add a repositories section to be able to resolve dependencies
        Files.write(
            new File(tmpDir.getRoot(), "build.gradle").toPath(),
            ("\n" +
                "repositories {\n" +
                "  maven {\n" +
                "    url \"" + getLocalTestRepoPath()  + "\"\n" +
                "  }\n" +
                "  maven {\n" +
                "    url \"http://s3.amazonaws.com/download.elasticsearch.org/lucenesnapshots/" + getLuceneSnapshotRevision() + "\"\n" +
                "  }\n" +
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

    private String getLuceneSnapshotRevision() {
        return System.getProperty("test.luceene-snapshot-revision", "not-a-snapshot");
    }

    private String getLocalTestRepoPath() {
        String property = System.getProperty("test.local-test-repo-path");
        Objects.requireNonNull(property, "test.local-test-repo-path not passed to tests");
        File file = new File(property);
        assertTrue("Expected " + property + " to exist, but it did not!", file.exists());
        return file.getAbsolutePath();
    }

}
