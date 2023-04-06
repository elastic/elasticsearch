/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.shard;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.lucene.tests.store.BaseDirectoryWrapper;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.rollover.Condition;
import org.elasticsearch.action.admin.indices.rollover.MaxAgeCondition;
import org.elasticsearch.action.admin.indices.rollover.MaxDocsCondition;
import org.elasticsearch.action.admin.indices.rollover.MaxPrimaryShardDocsCondition;
import org.elasticsearch.action.admin.indices.rollover.MaxPrimaryShardSizeCondition;
import org.elasticsearch.action.admin.indices.rollover.MaxSizeCondition;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.gateway.PersistedClusterStateService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.InternalEngineFactory;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.TestTranslog;
import org.elasticsearch.index.translog.TranslogCorruptedException;
import org.elasticsearch.test.CorruptionUtils;
import org.elasticsearch.test.DummyShardLock;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.index.shard.RemoveCorruptedShardDataCommand.TRUNCATE_CLEAN_TRANSLOG_FLAG;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

public class RemoveCorruptedShardDataCommandTests extends IndexShardTestCase {

    private ShardId shardId;
    private ShardRouting routing;
    private Environment environment;
    private ShardPath shardPath;
    private IndexMetadata indexMetadata;
    private ClusterState clusterState;
    private IndexShard indexShard;
    private Path[] dataPaths;
    private Path translogPath;
    private Path indexPath;
    private ProcessInfo processInfo;

    private static final Pattern NUM_CORRUPT_DOCS_PATTERN = Pattern.compile(
        "Corrupted Lucene index segments found -\\s+(?<docs>\\d+) documents will be lost."
    );

    @Before
    public void setup() throws IOException {
        shardId = new ShardId("index0", UUIDs.randomBase64UUID(), 0);
        final String nodeId = randomAlphaOfLength(10);
        routing = TestShardRouting.newShardRouting(
            shardId,
            nodeId,
            true,
            ShardRoutingState.INITIALIZING,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE
        );

        dataPaths = new Path[] { createTempDir(), createTempDir(), createTempDir() };
        final String[] tmpPaths = Arrays.stream(dataPaths).map(s -> s.toAbsolutePath().toString()).toArray(String[]::new);
        int randomPath = TestUtil.nextInt(random(), 0, dataPaths.length - 1);
        final Path tempDir = dataPaths[randomPath];

        environment = TestEnvironment.newEnvironment(
            Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), tempDir)
                .putList(Environment.PATH_DATA_SETTING.getKey(), tmpPaths)
                .build()
        );

        // create same directory structure as prod does
        for (Path dataPath : dataPaths) {
            Files.createDirectories(dataPath);
        }

        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_INDEX_UUID, shardId.getIndex().getUUID())
            .build();

        final NodeEnvironment.DataPath dataPath = new NodeEnvironment.DataPath(tempDir);
        shardPath = new ShardPath(false, dataPath.resolve(shardId), dataPath.resolve(shardId), shardId);

        // Adding rollover info to IndexMetadata to check that NamedXContentRegistry is properly configured
        Condition<?> rolloverCondition = randomFrom(
            new MaxAgeCondition(new TimeValue(randomNonNegativeLong())),
            new MaxDocsCondition(randomNonNegativeLong()),
            new MaxPrimaryShardSizeCondition(ByteSizeValue.ofBytes(randomNonNegativeLong())),
            new MaxSizeCondition(ByteSizeValue.ofBytes(randomNonNegativeLong())),
            new MaxPrimaryShardDocsCondition(randomNonNegativeLong())
        );

        final IndexMetadata.Builder metadata = IndexMetadata.builder(routing.getIndexName())
            .settings(settings)
            .primaryTerm(0, randomIntBetween(1, 100))
            .putRolloverInfo(new RolloverInfo("test", Collections.singletonList(rolloverCondition), randomNonNegativeLong()))
            .putMapping("{ \"properties\": {} }");
        indexMetadata = metadata.build();

        clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder().put(indexMetadata, false).build()).build();

        try (NodeEnvironment.NodeLock lock = new NodeEnvironment.NodeLock(logger, environment, Files::exists)) {
            final Path[] paths = Arrays.stream(lock.getDataPaths()).filter(Objects::nonNull).map(p -> p.path).toArray(Path[]::new);
            try (
                PersistedClusterStateService.Writer writer = new PersistedClusterStateService(
                    paths,
                    nodeId,
                    xContentRegistry(),
                    new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                    () -> 0L
                ).createWriter()
            ) {
                writer.writeFullStateAndCommit(1L, clusterState);
            }
        }

        indexShard = newStartedShard(
            p -> newShard(
                routing,
                shardPath,
                indexMetadata,
                null,
                null,
                new InternalEngineFactory(),
                () -> {},
                RetentionLeaseSyncer.EMPTY,
                EMPTY_EVENT_LISTENER
            ),
            true
        );

        translogPath = shardPath.resolveTranslog();
        indexPath = shardPath.resolveIndex();
        processInfo = new ProcessInfo(Map.of(), Map.of(), createTempDir());
    }

    public void testShardLock() throws Exception {
        indexDocs(indexShard, true);

        final RemoveCorruptedShardDataCommand command = new RemoveCorruptedShardDataCommand();
        final MockTerminal t = MockTerminal.create();
        final OptionParser parser = command.getParser();

        // Try running it before the shard is closed, it should flip out because it can't acquire the lock
        try {
            final OptionSet options = parser.parse("-d", indexPath.toString());
            command.execute(t, options, environment, processInfo);
            fail("expected the command to fail not being able to acquire the lock");
        } catch (Exception e) {
            assertThat(e.getMessage(), containsString("Failed to lock shard's directory"));
        }

        // close shard
        closeShards(indexShard);

        // Try running it before the shard is corrupted
        try {
            final OptionSet options = parser.parse("-d", indexPath.toString());
            command.execute(t, options, environment, processInfo);
            fail("expected the command to fail not being able to find a corrupt file marker");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), startsWith("Shard does not seem to be corrupted at"));
            assertThat(t.getOutput(), containsString("Lucene index is clean at"));
        }
    }

    public void testCorruptedIndex() throws Exception {
        final int numDocs = indexDocs(indexShard, true);

        // close shard
        closeShards(indexShard);

        final boolean corruptSegments = randomBoolean();
        CorruptionUtils.corruptIndex(random(), indexPath, corruptSegments);

        if (randomBoolean()) {
            // test corrupted shard and add corruption marker
            final IndexShard corruptedShard = reopenIndexShard(true);
            allowShardFailures();
            expectThrows(IndexShardRecoveryException.class, () -> newStartedShard(p -> corruptedShard, true));
            closeShards(corruptedShard);
        }

        final RemoveCorruptedShardDataCommand command = new RemoveCorruptedShardDataCommand();
        final MockTerminal t = MockTerminal.create();
        final OptionParser parser = command.getParser();

        // run command with dry-run
        t.addTextInput("n"); // mean dry run
        final OptionSet options = parser.parse("-d", indexPath.toString());
        t.setVerbosity(Terminal.Verbosity.VERBOSE);
        try {
            command.execute(t, options, environment, processInfo);
            fail();
        } catch (ElasticsearchException e) {
            if (corruptSegments) {
                assertThat(e.getMessage(), either(is("Index is unrecoverable")).or(startsWith("unable to list commits")));
            } else {
                assertThat(e.getMessage(), containsString("aborted by user"));
            }
        } finally {
            logger.info("--> output:\n{}", t.getOutput());
        }

        if (corruptSegments == false) {

            // run command without dry-run
            t.addTextInput("y");
            command.execute(t, options, environment, processInfo);

            final String output = t.getOutput();
            logger.info("--> output:\n{}", output);

            // reopen shard
            failOnShardFailures();
            final IndexShard newShard = newStartedShard(p -> reopenIndexShard(false), true);

            final Set<String> shardDocUIDs = getShardDocUIDs(newShard);

            final Matcher matcher = NUM_CORRUPT_DOCS_PATTERN.matcher(output);
            assertThat(matcher.find(), equalTo(true));
            final int expectedNumDocs = numDocs - Integer.parseInt(matcher.group("docs"));

            assertThat(shardDocUIDs.size(), equalTo(expectedNumDocs));

            closeShards(newShard);
        }
    }

    public void testCorruptedTranslog() throws Exception {
        final int numDocsToKeep = indexDocs(indexShard, false);

        // close shard
        closeShards(indexShard);

        TestTranslog.corruptRandomTranslogFile(logger, random(), translogPath);

        // test corrupted shard
        final IndexShard corruptedShard = reopenIndexShard(true);

        allowShardFailures();
        // it has to fail on start up due to index.shard.check_on_startup = checksum
        final Exception exception = expectThrows(Exception.class, () -> newStartedShard(p -> corruptedShard, true));
        final Throwable cause = exception.getCause() instanceof EngineException ? exception.getCause().getCause() : exception.getCause();
        assertThat(cause, instanceOf(TranslogCorruptedException.class));

        closeShard(corruptedShard, false); // translog is corrupted already - do not check consistency

        final RemoveCorruptedShardDataCommand command = new RemoveCorruptedShardDataCommand();
        final MockTerminal t = MockTerminal.create();
        final OptionParser parser = command.getParser();

        final OptionSet options = parser.parse("-d", translogPath.toString());
        // run command with dry-run
        t.addTextInput("n"); // mean dry run
        t.setVerbosity(Terminal.Verbosity.VERBOSE);
        try {
            command.execute(t, options, environment, processInfo);
            fail();
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), containsString("aborted by user"));
            assertThat(t.getOutput(), containsString("Continue and remove corrupted data from the shard ?"));
        }

        logger.info("--> output:\n{}", t.getOutput());

        // run command without dry-run
        t.reset();
        t.addTextInput("y");
        command.execute(t, options, environment, processInfo);

        final String output = t.getOutput();
        logger.info("--> output:\n{}", output);

        // reopen shard
        failOnShardFailures();
        final IndexShard newShard = newStartedShard(p -> reopenIndexShard(false), true);

        final Set<String> shardDocUIDs = getShardDocUIDs(newShard);

        assertThat(shardDocUIDs.size(), equalTo(numDocsToKeep));

        closeShards(newShard);
    }

    public void testCorruptedBothIndexAndTranslog() throws Exception {
        // index some docs in several segments
        final int numDocsToKeep = indexDocs(indexShard, false);

        // close shard
        closeShards(indexShard);

        CorruptionUtils.corruptIndex(random(), indexPath, false);

        if (randomBoolean()) {
            // test corrupted shard and add corruption marker
            final IndexShard corruptedShard = reopenIndexShard(true);
            allowShardFailures();
            expectThrows(IndexShardRecoveryException.class, () -> newStartedShard(p -> corruptedShard, true));
            closeShards(corruptedShard);
        }
        TestTranslog.corruptRandomTranslogFile(logger, random(), translogPath);

        final RemoveCorruptedShardDataCommand command = new RemoveCorruptedShardDataCommand();
        final MockTerminal t = MockTerminal.create();
        final OptionParser parser = command.getParser();

        final OptionSet options = parser.parse("-d", translogPath.toString());
        // run command with dry-run
        t.addTextInput("n"); // mean dry run
        t.addTextInput("n"); // mean dry run
        t.setVerbosity(Terminal.Verbosity.VERBOSE);
        try {
            command.execute(t, options, environment, processInfo);
            fail();
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), containsString("aborted by user"));
            assertThat(t.getOutput(), containsString("Continue and remove corrupted data from the shard ?"));
        }

        logger.info("--> output:\n{}", t.getOutput());

        // run command without dry-run
        t.reset();
        t.addTextInput("y");
        command.execute(t, options, environment, processInfo);

        final String output = t.getOutput();
        logger.info("--> output:\n{}", output);

        // reopen shard
        failOnShardFailures();
        final IndexShard newShard = newStartedShard(p -> reopenIndexShard(false), true);

        final Set<String> shardDocUIDs = getShardDocUIDs(newShard);

        final Matcher matcher = NUM_CORRUPT_DOCS_PATTERN.matcher(output);
        assertThat(matcher.find(), equalTo(true));
        final int expectedNumDocs = numDocsToKeep - Integer.parseInt(matcher.group("docs"));

        assertThat(shardDocUIDs.size(), equalTo(expectedNumDocs));

        closeShards(newShard);
    }

    public void testResolveIndexDirectory() throws Exception {
        // index a single doc to have files on a disk
        indexDoc(indexShard, "_doc", "0", "{}");
        flushShard(indexShard, true);

        // close shard
        closeShards(indexShard);

        final RemoveCorruptedShardDataCommand command = new RemoveCorruptedShardDataCommand();
        final OptionParser parser = command.getParser();

        // `--index index_name --shard-id 0` has to be resolved to indexPath
        final OptionSet options = parser.parse("--index", shardId.getIndex().getName(), "--shard-id", Integer.toString(shardId.id()));

        command.findAndProcessShardPath(
            options,
            environment,
            dataPaths,
            clusterState,
            shardPath -> assertThat(shardPath.resolveIndex(), equalTo(indexPath))
        );

        final OptionSet options2 = parser.parse("--dir", indexPath.toAbsolutePath().toString());
        command.findAndProcessShardPath(
            options2,
            environment,
            dataPaths,
            clusterState,
            shardPath -> assertThat(shardPath.resolveIndex(), equalTo(indexPath))
        );
    }

    public void testFailsOnCleanIndex() throws Exception {
        indexDocs(indexShard, true);
        closeShards(indexShard);

        final RemoveCorruptedShardDataCommand command = new RemoveCorruptedShardDataCommand();
        final MockTerminal t = MockTerminal.create();
        final OptionParser parser = command.getParser();

        final OptionSet options = parser.parse("-d", translogPath.toString());
        t.setVerbosity(Terminal.Verbosity.VERBOSE);
        assertThat(
            expectThrows(ElasticsearchException.class, () -> command.execute(t, options, environment, processInfo)).getMessage(),
            allOf(containsString("Shard does not seem to be corrupted"), containsString("--" + TRUNCATE_CLEAN_TRANSLOG_FLAG))
        );
        assertThat(t.getOutput(), containsString("Lucene index is clean"));
        assertThat(t.getOutput(), containsString("Translog is clean"));
    }

    public void testTruncatesCleanTranslogIfRequested() throws Exception {
        indexDocs(indexShard, true);
        closeShards(indexShard);

        final RemoveCorruptedShardDataCommand command = new RemoveCorruptedShardDataCommand();
        final MockTerminal t = MockTerminal.create();
        final OptionParser parser = command.getParser();

        final OptionSet options = parser.parse("-d", translogPath.toString(), "--" + TRUNCATE_CLEAN_TRANSLOG_FLAG);
        t.addTextInput("y");
        t.setVerbosity(Terminal.Verbosity.VERBOSE);
        command.execute(t, options, environment, processInfo);
        assertThat(t.getOutput(), containsString("Lucene index is clean"));
        assertThat(t.getOutput(), containsString("Translog was not analysed and will be truncated"));
        assertThat(t.getOutput(), containsString("Creating new empty translog"));
    }

    public void testCleanWithCorruptionMarker() throws Exception {
        // index some docs in several segments
        final int numDocs = indexDocs(indexShard, true);

        indexShard.store().markStoreCorrupted(null);

        closeShards(indexShard);

        allowShardFailures();
        final IndexShard corruptedShard = reopenIndexShard(true);
        expectThrows(IndexShardRecoveryException.class, () -> newStartedShard(p -> corruptedShard, true));
        closeShards(corruptedShard);

        final RemoveCorruptedShardDataCommand command = new RemoveCorruptedShardDataCommand();
        final MockTerminal t = MockTerminal.create();
        final OptionParser parser = command.getParser();

        final OptionSet options = parser.parse("-d", translogPath.toString());
        // run command with dry-run
        t.addTextInput("n"); // mean dry run
        t.addTextInput("n"); // mean dry run
        t.setVerbosity(Terminal.Verbosity.VERBOSE);
        try {
            command.execute(t, options, environment, processInfo);
            fail();
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), containsString("aborted by user"));
            assertThat(t.getOutput(), containsString("Continue and remove corrupted data from the shard ?"));
            assertThat(t.getOutput(), containsString("Lucene index is marked corrupted, but no corruption detected"));
        }

        logger.info("--> output:\n{}", t.getOutput());

        // run command without dry-run
        t.reset();
        t.addTextInput("y");
        t.addTextInput("y");
        command.execute(t, options, environment, processInfo);

        final String output = t.getOutput();
        logger.info("--> output:\n{}", output);

        failOnShardFailures();
        final IndexShard newShard = newStartedShard(p -> reopenIndexShard(false), true);

        final Set<String> shardDocUIDs = getShardDocUIDs(newShard);
        assertEquals(numDocs, shardDocUIDs.size());

        assertThat(t.getOutput(), containsString("This shard has been marked as corrupted but no corruption can now be detected."));

        final Matcher matcher = NUM_CORRUPT_DOCS_PATTERN.matcher(output);
        assertFalse(matcher.find());

        closeShards(newShard);
    }

    private IndexShard reopenIndexShard(boolean corrupted) throws IOException {
        // open shard with the same location
        final ShardRouting shardRouting = ShardRoutingHelper.initWithSameId(
            indexShard.routingEntry(),
            RecoverySource.ExistingStoreRecoverySource.INSTANCE
        );

        final IndexMetadata metadata = IndexMetadata.builder(indexMetadata)
            .settings(
                Settings.builder()
                    .put(indexShard.indexSettings().getSettings())
                    .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), "checksum")
            )
            .build();

        CheckedFunction<IndexSettings, Store, IOException> storeProvider = corrupted == false ? null : indexSettings -> {
            final ShardId shardId = shardPath.getShardId();
            final BaseDirectoryWrapper baseDirectoryWrapper = newFSDirectory(shardPath.resolveIndex());
            // index is corrupted - don't even try to check index on close - it fails
            baseDirectoryWrapper.setCheckIndexOnClose(false);
            return new Store(shardId, indexSettings, baseDirectoryWrapper, new DummyShardLock(shardId));
        };

        return newShard(
            shardRouting,
            shardPath,
            metadata,
            storeProvider,
            null,
            indexShard.engineFactory,
            indexShard.getGlobalCheckpointSyncer(),
            indexShard.getRetentionLeaseSyncer(),
            EMPTY_EVENT_LISTENER
        );
    }

    private int indexDocs(IndexShard indexShard, boolean flushLast) throws IOException {
        // index some docs in several segments
        int numDocs = 0;
        int numDocsToKeep = 0;
        for (int i = 0, attempts = randomIntBetween(5, 10); i < attempts; i++) {
            final int numExtraDocs = between(10, 100);
            for (long j = 0; j < numExtraDocs; j++) {
                indexDoc(indexShard, "_doc", Long.toString(numDocs + j), "{}");
            }
            numDocs += numExtraDocs;

            if (flushLast || i < attempts - 1) {
                numDocsToKeep += numExtraDocs;
                flushShard(indexShard, true);
            }
        }

        logger.info("--> indexed {} docs, {} to keep", numDocs, numDocsToKeep);

        return numDocsToKeep;
    }

}
