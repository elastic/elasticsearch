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
package org.elasticsearch.test.plugins;


import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SuppressForbidden;
import org.gradle.testkit.runner.GradleRunner;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;


@RunWith(RandomizedRunner.class)
@SuppressForbidden(reason = "FileUtils,  File.separator, File.pathSeparator")
public class BuildExamplePluginsIT extends Assert {

    protected final Logger logger = LogManager.getLogger(getClass());

    private static final List<Path> EXAMPLE_PLUGINS = Collections.unmodifiableList(
        Arrays.stream(
            Objects.requireNonNull(System.getProperty("test.build-tools.plugin.examples"))
                .split(File.pathSeparator)
        ).map(Paths::get).collect(Collectors.toList())
    );

    private static final String BUILD_TOOLS_VERSION = Objects.requireNonNull(System.getProperty("test.version_under_test"));

    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    public final Path examplePlugin;

    public BuildExamplePluginsIT(Path examplePlugin) {
        this.examplePlugin = examplePlugin;
    }

    @BeforeClass
    public static void assertProjectsExist() {
        assertEquals(
            EXAMPLE_PLUGINS,
            EXAMPLE_PLUGINS.stream().filter(Files::exists).collect(Collectors.toList())
        );
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return EXAMPLE_PLUGINS
            .stream()
            .map(each -> new Object[] {each})
            .collect(Collectors.toList());
    }


    @Test
    public void testCurrentExamplePlugin() throws IOException {
        FileUtils.copyDirectory(examplePlugin.toFile(), tmpDir.getRoot(), pathname -> pathname.getPath().contains("/build/") == false);

        adaptBuildScriptForTest();

        Files.write(
            tmpDir.newFile("NOTICE.txt").toPath(),
            "dummy test notice".getBytes(StandardCharsets.UTF_8)
        );
        Files.write(
            tmpDir.newFile("settings.gradle").toPath(),
            "".getBytes(StandardCharsets.UTF_8)
        );


        GradleRunner.create()
            .withProjectDir(tmpDir.getRoot())
            .withTestKitDir(Files.createTempDirectory("gradle-testkit").toAbsolutePath().toFile())
            .withArguments("clean", "assemble")
            .build();
    }

    private void adaptBuildScriptForTest() throws IOException {
        // Add the local repo as a build script URL so we can pull in build-tools and apply the plugin under test
        // we need to specify the exact version of build-tools because gradle automatically adds its plugin portal
        // which appears to mirror jcenter, opening us up to pulling a "later" version of build-tools
        writeBuildScript(
            "buildscript {\n" +
                "    repositories {\n" +
                "        maven {\n" +
                "            name = \"test\"\n" +
                "            url = '" + getLocalTestRepoPath() + "'\n" +
                "        }\n" +
                "        jcenter()\n" +
                "    }\n" +
                "    dependencies {\n" +
                "        classpath \"org.elasticsearch.gradle:build-tools:" + BUILD_TOOLS_VERSION + "\"\n" +
                "    }\n" +
                "}\n"
        );
        // get the original file
        Files.readAllLines(getTempPath("build.gradle"), StandardCharsets.UTF_8)
            .stream()
            .map(line -> line + "\n")
            .forEach(this::writeBuildScript);
        // Add a repositories section to be able to resolve dependencies
        String luceneSnapshotRepo = "";
        String luceneSnapshotRevision = System.getProperty("test.lucene-snapshot-revision");
        if (luceneSnapshotRepo != null) {
            luceneSnapshotRepo =  "  maven {\n" +
                "    name \"lucene-snapshots\"\n" +
                "    url \"https://s3.amazonaws.com/download.elasticsearch.org/lucenesnapshots/" + luceneSnapshotRevision + "\"\n" +
                "  }\n";
        }
        writeBuildScript("\n" +
                "repositories {\n" +
                "  maven {\n" +
                "    name \"test\"\n" +
                "    url \"" + getLocalTestRepoPath()  + "\"\n" +
                "  }\n" +
                "  jcenter() \n" +
                luceneSnapshotRepo +
                "}\n"
        );
        Files.delete(getTempPath("build.gradle"));
        Files.move(getTempPath("build.gradle.new"), getTempPath("build.gradle"));

        logger.info("Generated build script is:");
        Files.readAllLines(getTempPath("build.gradle")).forEach(logger::info);
    }

    private Path getTempPath(String fileName) {
        return new File(tmpDir.getRoot(), fileName).toPath();
    }

    private Path writeBuildScript(String script) {
        try {
            Path path = getTempPath("build.gradle.new");
            return Files.write(
                path,
                script.getBytes(StandardCharsets.UTF_8),
                Files.exists(path) ? StandardOpenOption.APPEND : StandardOpenOption.CREATE_NEW
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected String getLocalTestRepoPath() {
        return getLocalTestPath("test.local-test-repo-path");
    }

    private String getLocalTestPath(String propertyName) {
        String property = System.getProperty(propertyName);
        Objects.requireNonNull(property, propertyName + " not passed to tests");
        Path file = Paths.get(property);
        assertTrue("Expected " + property + " to exist, but it did not!", Files.exists(file));
        if (File.separator.equals("\\")) {
            // Use / on Windows too, the build script is not happy with \
            return file.toAbsolutePath().toString().replace(File.separator, "/");
        } else {
            return file.toAbsolutePath().toString();
        }
    }
}
