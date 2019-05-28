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
package org.elasticsearch.env;

import joptsimple.OptionSet;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.Manifest;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.CheckedRunnable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.Index;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;

import static org.elasticsearch.env.NodeRepurposeCommand.NO_CLEANUP;
import static org.elasticsearch.env.NodeRepurposeCommand.NO_DATA_TO_CLEAN_UP_FOUND;
import static org.elasticsearch.env.NodeRepurposeCommand.NO_SHARD_DATA_TO_CLEAN_UP_FOUND;
import static org.elasticsearch.env.NodeRepurposeCommand.PRE_V7_MESSAGE;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class NodeRepurposeCommandTests extends ESTestCase {

    private static final Index INDEX = new Index("testIndex", "testUUID");
    private Settings dataMasterSettings;
    private Environment environment;
    private Path[] nodePaths;
    private Settings dataNoMasterSettings;
    private Settings noDataNoMasterSettings;
    private Settings noDataMasterSettings;

    @Before
    public void createNodePaths() throws IOException {
        dataMasterSettings = buildEnvSettings(Settings.EMPTY);
        environment = TestEnvironment.newEnvironment(dataMasterSettings);
        try (NodeEnvironment nodeEnvironment = new NodeEnvironment(dataMasterSettings, environment)) {
            nodePaths = nodeEnvironment.nodeDataPaths();
        }
        dataNoMasterSettings = Settings.builder()
            .put(dataMasterSettings)
            .put(Node.NODE_MASTER_SETTING.getKey(), false)
            .build();
        noDataNoMasterSettings = Settings.builder()
            .put(dataMasterSettings)
            .put(Node.NODE_DATA_SETTING.getKey(), false)
            .put(Node.NODE_MASTER_SETTING.getKey(), false)
            .build();
        noDataMasterSettings = Settings.builder()
            .put(dataMasterSettings)
            .put(Node.NODE_DATA_SETTING.getKey(), false)
            .put(Node.NODE_MASTER_SETTING.getKey(), true)
            .build();
    }

    public void testEarlyExitNoCleanup() throws Exception {
        createIndexDataFiles(dataMasterSettings, randomInt(10));

        verifyNoQuestions(dataMasterSettings, containsString(NO_CLEANUP));
        verifyNoQuestions(dataNoMasterSettings, containsString(NO_CLEANUP));
    }

    public void testNothingToCleanup() throws Exception {
        verifyNoQuestions(noDataNoMasterSettings, allOf(containsString(NO_DATA_TO_CLEAN_UP_FOUND), not(containsString(PRE_V7_MESSAGE))));
        verifyNoQuestions(noDataMasterSettings,
            allOf(containsString(NO_SHARD_DATA_TO_CLEAN_UP_FOUND), not(containsString(PRE_V7_MESSAGE))));

        createManifest(null);

        verifyNoQuestions(noDataNoMasterSettings, allOf(containsString(NO_DATA_TO_CLEAN_UP_FOUND), not(containsString(PRE_V7_MESSAGE))));
        verifyNoQuestions(noDataMasterSettings,
            allOf(containsString(NO_SHARD_DATA_TO_CLEAN_UP_FOUND), not(containsString(PRE_V7_MESSAGE))));

        createIndexDataFiles(dataMasterSettings, 0);

        verifyNoQuestions(noDataMasterSettings,
            allOf(containsString(NO_SHARD_DATA_TO_CLEAN_UP_FOUND), not(containsString(PRE_V7_MESSAGE))));

    }

    public void testLocked() throws IOException {
        try (NodeEnvironment env = new NodeEnvironment(dataMasterSettings, TestEnvironment.newEnvironment(dataMasterSettings))) {
            assertThat(expectThrows(ElasticsearchException.class,
                () -> verifyNoQuestions(noDataNoMasterSettings, null)).getMessage(),
                containsString(NodeRepurposeCommand.FAILED_TO_OBTAIN_NODE_LOCK_MSG));
        }
    }

    public void testCleanupAll() throws Exception {
        Manifest oldManifest = createManifest(INDEX);
        checkCleanupAll(not(containsString(PRE_V7_MESSAGE)));

        Manifest newManifest = loadManifest();
        assertThat(newManifest.getIndexGenerations().entrySet(), hasSize(0));
        assertManifestIdenticalExceptIndices(oldManifest, newManifest);
    }

    public void testCleanupAllPreV7() throws Exception {
        checkCleanupAll(containsString(PRE_V7_MESSAGE));
    }

    private void checkCleanupAll(Matcher<String> additionalOutputMatcher) throws Exception {
        int shardCount = randomInt(10);
        boolean verbose = randomBoolean();
        createIndexDataFiles(dataMasterSettings, shardCount);

        String messageText = NodeRepurposeCommand.noMasterMessage(
            1,
            environment.dataFiles().length*shardCount,
            environment.dataFiles().length);

        Matcher<String> outputMatcher = allOf(
            containsString(messageText),
            additionalOutputMatcher,
            conditionalNot(containsString("testUUID"), verbose == false),
            conditionalNot(containsString("testIndex"), verbose == false)
        );

        verifyUnchangedOnAbort(noDataNoMasterSettings, outputMatcher, verbose);

        // verify test setup
        expectThrows(IllegalStateException.class, () -> new NodeEnvironment(noDataNoMasterSettings, environment).close());

        verifySuccess(noDataNoMasterSettings, outputMatcher, verbose);

        //verify cleaned.
        new NodeEnvironment(noDataNoMasterSettings, environment).close();
    }

    public void testCleanupShardData() throws Exception {
        int shardCount = randomIntBetween(1, 10);
        boolean verbose = randomBoolean();
        Manifest manifest = randomBoolean() ? createManifest(INDEX) : null;

        createIndexDataFiles(dataMasterSettings, shardCount);

        Matcher<String> matcher = allOf(
            containsString(NodeRepurposeCommand.shardMessage(environment.dataFiles().length * shardCount, 1)),
            conditionalNot(containsString("testUUID"), verbose == false),
            conditionalNot(containsString("testIndex"), verbose == false)
        );

        verifyUnchangedOnAbort(noDataMasterSettings,
            matcher, verbose);

        // verify test setup
        expectThrows(IllegalStateException.class, () -> new NodeEnvironment(noDataMasterSettings, environment).close());

        verifySuccess(noDataMasterSettings, matcher, verbose);

        //verify clean.
        new NodeEnvironment(noDataMasterSettings, environment).close();

        if (manifest != null) {
            Manifest newManifest = loadManifest();
            assertThat(newManifest.getIndexGenerations().entrySet(), hasSize(1));
            assertManifestIdenticalExceptIndices(manifest, newManifest);
        }
    }

    static void verifySuccess(Settings settings, Matcher<String> outputMatcher, boolean verbose) throws Exception {
        withTerminal(verbose, outputMatcher, terminal -> {
            terminal.addTextInput(randomFrom("y", "Y"));
            executeRepurposeCommand(terminal, settings, 0);
            assertThat(terminal.getOutput(), containsString("Node successfully repurposed"));
        });
    }

    private void verifyUnchangedOnAbort(Settings settings, Matcher<String> outputMatcher, boolean verbose) throws Exception {
        withTerminal(verbose, outputMatcher, terminal -> {
            terminal.addTextInput(randomFrom("yy", "Yy", "n", "yes", "true", "N", "no"));
            verifyUnchangedDataFiles(() -> {
                ElasticsearchException exception = expectThrows(ElasticsearchException.class,
                    () -> executeRepurposeCommand(terminal, settings, 0));
                assertThat(exception.getMessage(), containsString(NodeRepurposeCommand.ABORTED_BY_USER_MSG));
            });
        });
    }

    private void verifyNoQuestions(Settings settings, Matcher<String> outputMatcher) throws Exception {
        withTerminal(false, outputMatcher, terminal -> {
            executeRepurposeCommand(terminal, settings, 0);
        });
    }

    private static void withTerminal(boolean verbose, Matcher<String> outputMatcher,
                                     CheckedConsumer<MockTerminal, Exception> consumer) throws Exception {
        MockTerminal terminal = new MockTerminal();
        if (verbose) {
            terminal.setVerbosity(Terminal.Verbosity.VERBOSE);
        }

        consumer.accept(terminal);

        assertThat(terminal.getOutput(), outputMatcher);

        expectThrows(IllegalStateException.class, "Must consume input", () -> terminal.readText(""));
    }

    private static void executeRepurposeCommand(MockTerminal terminal, Settings settings, int ordinal) throws Exception {
        NodeRepurposeCommand nodeRepurposeCommand = new NodeRepurposeCommand();
        OptionSet options = nodeRepurposeCommand.getParser()
            .parse(ordinal != 0 ? new String[]{"--ordinal", Integer.toString(ordinal)} : new String[0]);
        Environment env = TestEnvironment.newEnvironment(settings);
        nodeRepurposeCommand.testExecute(terminal, options, env);
    }

    private Manifest createManifest(Index index) throws org.elasticsearch.gateway.WriteStateException {
        Manifest manifest = new Manifest(randomIntBetween(1,100), randomIntBetween(1,100), randomIntBetween(1,100),
            index != null ? Collections.singletonMap(index, randomLongBetween(1,100)) : Collections.emptyMap());
        Manifest.FORMAT.writeAndCleanup(manifest, nodePaths);
        return manifest;
    }

    private Manifest loadManifest() throws IOException {
        return Manifest.FORMAT.loadLatestState(logger, new NamedXContentRegistry(ClusterModule.getNamedXWriteables()), nodePaths);
    }

    private void assertManifestIdenticalExceptIndices(Manifest oldManifest, Manifest newManifest) {
        assertEquals(oldManifest.getGlobalGeneration(), newManifest.getGlobalGeneration());
        assertEquals(oldManifest.getClusterStateVersion(), newManifest.getClusterStateVersion());
        assertEquals(oldManifest.getCurrentTerm(), newManifest.getCurrentTerm());
    }

    private void createIndexDataFiles(Settings settings, int shardCount) throws IOException {
        int shardDataDirNumber = randomInt(10);
        try (NodeEnvironment env = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings))) {
            IndexMetaData.FORMAT.write(IndexMetaData.builder(INDEX.getName())
                .settings(Settings.builder().put("index.version.created", Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .build(), env.indexPaths(INDEX));
            for (Path path : env.indexPaths(INDEX)) {
                for (int i = 0; i < shardCount; ++i) {
                    Files.createDirectories(path.resolve(Integer.toString(shardDataDirNumber)));
                    shardDataDirNumber += randomIntBetween(1,10);
                }
            }
        }
    }

    private void verifyUnchangedDataFiles(CheckedRunnable<? extends Exception> runnable) throws Exception {
        long before = digestPaths();
        runnable.run();
        long after = digestPaths();
        assertEquals("Must not touch files", before, after);
    }

    private long digestPaths() {
        // use a commutative digest to avoid dependency on file system order.
        return Arrays.stream(environment.dataFiles()).mapToLong(this::digestPath).sum();
    }

    private long digestPath(Path path) {
        try (Stream<Path> paths = Files.walk(path)) {
            return paths.mapToLong(this::digestSinglePath).sum();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private long digestSinglePath(Path path) {
        if (Files.isDirectory(path))
            return path.toString().hashCode();
        else
            return path.toString().hashCode() + digest(readAllBytes(path));

    }

    private byte[] readAllBytes(Path path) {
        try {
            return Files.readAllBytes(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private long digest(byte[] bytes) {
        long result = 0;
        for (byte b : bytes) {
            result *= 31;
            result += b;
        }
        return result;
    }

    static <T> Matcher<T> conditionalNot(Matcher<T> matcher, boolean condition) {
        return condition ? not(matcher) : matcher;
    }
}
