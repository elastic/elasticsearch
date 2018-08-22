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
package org.elasticsearch.index.shard;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.NativeFSLockFactory;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.engine.InternalEngineFactory;
import org.elasticsearch.index.store.DirectoryService;
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
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

public class ResolveShardCorruptionCommandTests extends IndexShardTestCase {

    private ShardId shardId;
    private ShardRouting routing;
    private Path dataDir;
    private Environment environment;
    private Settings settings;
    private ShardPath shardPath;
    private IndexMetaData indexMetaData;

    @Before
    public void setup() throws IOException {
        shardId = new ShardId("index0", "_na_", 0);
        final String nodeId = randomAlphaOfLength(10);
        routing = TestShardRouting.newShardRouting(shardId, nodeId, true, ShardRoutingState.INITIALIZING,
            RecoverySource.StoreRecoverySource.EMPTY_STORE_INSTANCE);

        dataDir = createTempDir();

        environment =
            TestEnvironment.newEnvironment(Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), dataDir)
                .putList(Environment.PATH_DATA_SETTING.getKey(), dataDir.toAbsolutePath().toString()).build());

        // create same directory structure as prod does
        final Path path = NodeEnvironment.resolveNodePath(dataDir, 0);
        Files.createDirectories(path);
        settings = Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();

        final NodeEnvironment.NodePath nodePath = new NodeEnvironment.NodePath(path);
        shardPath = new ShardPath(false, nodePath.resolve(shardId), nodePath.resolve(shardId), shardId);
        final IndexMetaData.Builder metaData = IndexMetaData.builder(routing.getIndexName())
            .settings(settings)
            .primaryTerm(0, randomIntBetween(1, 100))
            .putMapping("_doc", "{ \"properties\": {} }");
        indexMetaData = metaData.build();
    }

    public void testCorruptedIndex() throws Exception {
        final IndexShard indexShard = newStartedShard(p ->
                newShard(routing, shardPath, indexMetaData, null, null,
                    new InternalEngineFactory(), () -> {}, EMPTY_EVENT_LISTENER),
            true);

        // index some docs in several segments
        int numDocs = 0;
        for (int i = 0, attempts = randomIntBetween(5, 10); i < attempts; i++) {
            final int numExtraDocs = between(10, 100);
            for (long j = 0; j < numExtraDocs; j++) {
                indexDoc(indexShard, "_doc", Long.toString(numDocs + j), "{}");
            }
            numDocs += numExtraDocs;

            flushShard(indexShard, true);
        }

        logger.info("--> indexed {} docs", numDocs);

        writeIndexState();

        final ShardPath shardPath = indexShard.shardPath();
        final Path indexPath = shardPath.getDataPath().resolve(ShardPath.INDEX_FOLDER_NAME);

        final ResolveShardCorruptionCommand command = new ResolveShardCorruptionCommand();
        final MockTerminal t = new MockTerminal();
        final OptionParser parser = command.getParser();

        // Try running it before the shard is closed, it should flip out because it can't acquire the lock
        try {
            final OptionSet options = parser.parse("-d", indexPath.toString());
            command.execute(t, options, environment);
            fail("expected the command to fail not being able to acquire the lock");
        } catch (Exception e) {
            assertThat(e.getMessage(), containsString("Failed to lock shard's directory"));
        }

        // close shard
        closeShards(indexShard);

        // Try running it before the shard is corrupted
        try {
            final OptionSet options = parser.parse("-d", indexPath.toString());
            command.execute(t, options, environment);
            fail("expected the command to fail not being able to find a corrupt file marker");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(),
                allOf(
                    startsWith("Both Lucene index and traslog at"),
                    endsWith(" are clean.")
                ));
            assertThat(t.getOutput(), containsString("Lucene index is clean at"));
        }

        final boolean corruptSegments = randomBoolean();
        corruptIndexFiles(indexPath, corruptSegments);

        // open shard with the same location
        final ShardRouting shardRouting = ShardRoutingHelper.initWithSameId(indexShard.routingEntry(),
            RecoverySource.StoreRecoverySource.EXISTING_STORE_INSTANCE
        );

        final IndexMetaData indexMetaData = IndexMetaData.builder(indexShard.indexSettings().getIndexMetaData())
            .settings(Settings.builder()
                .put(indexShard.indexSettings().getSettings())
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), "checksum"))
            .build();

        final IndexShard corruptedShard = newShard(shardRouting, shardPath, indexMetaData,
            indexSettings -> {
                final ShardId shardId = shardPath.getShardId();
                final DirectoryService directoryService = new DirectoryService(shardId, indexSettings) {
                    @Override
                    public Directory newDirectory() throws IOException {
                        final BaseDirectoryWrapper baseDirectoryWrapper = newFSDirectory(shardPath.resolveIndex());
                        // index is corrupted - don't even try to check index on close - it fails
                        baseDirectoryWrapper.setCheckIndexOnClose(false);
                        return baseDirectoryWrapper;
                    }
                };
                return new Store(shardId, indexSettings, directoryService, new DummyShardLock(shardId));
            },
            null,
            indexShard.engineFactory,
            indexShard.getGlobalCheckpointSyncer(), EMPTY_EVENT_LISTENER);

        // it has to fail on start up due to index.shard.check_on_startup = checksum
        expectThrows(IndexShardRecoveryException.class, () -> newStartedShard(p -> corruptedShard, true));

        closeShards(corruptedShard);

        // checking that lock has been released

        try (Directory dir = FSDirectory.open(indexPath, NativeFSLockFactory.INSTANCE);
             Lock writeLock = dir.obtainLock(IndexWriter.WRITE_LOCK_NAME)) {
            // Great, do nothing, we just wanted to obtain the lock
        }

        // index is closed, lock is released - run remove-corrupted-segments command with dry-run
        t.addTextInput("n"); // mean dry run
        final OptionSet options = parser.parse("-d", indexPath.toString());
        t.setVerbosity(Terminal.Verbosity.VERBOSE);
        try {
            command.execute(t, options, environment);
            fail();
        } catch (ElasticsearchException e) {
            if (corruptSegments) {
                assertThat(e.getMessage(), is("Index is unrecoverable - there are missing segments"));
            } else {
                assertThat(e.getMessage(), containsString("aborted by user"));
            }
        }

        logger.info("--> output:\n{}", t.getOutput());

        if (corruptSegments == false) {

            // index is closed, lock is released - run remove-corrupted-segments command
            t.addTextInput("y");
            t.addTextInput("y");
            command.execute(t, options, environment);

            final String output = t.getOutput();
            logger.info("--> output:\n{}", output);

            // reopen shard, do checksum on start up

            final IndexShard newShard = newStartedShard(p ->
                    newShard(shardRouting, shardPath, indexMetaData,
                        null, null, indexShard.engineFactory,
                        indexShard.getGlobalCheckpointSyncer(), EMPTY_EVENT_LISTENER),
                true);

            final Set<String> shardDocUIDs = getShardDocUIDs(newShard);

            final Pattern pattern = Pattern.compile("Corrupted segments found -\\s+(?<docs>\\d+) documents will be lost.");
            final Matcher matcher = pattern.matcher(output);
            assertThat(matcher.find(), equalTo(true));
            final int expectedNumDocs = numDocs - Integer.parseInt(matcher.group("docs"));

            assertThat(shardDocUIDs.size(), equalTo(expectedNumDocs));

            closeShards(newShard);
        }
    }

    public void testCorruptedTranslog() throws Exception {
        final InternalEngineFactory engineFactory = new InternalEngineFactory();
        final Runnable globalCheckpointSyncer = () -> { };
        final IndexShard indexShard = newStartedShard(p ->
                newShard(routing, shardPath, indexMetaData, null, null,
                    engineFactory, globalCheckpointSyncer, EMPTY_EVENT_LISTENER),
            true);

        // index some docs in several segments
        int numDocs = 0;
        int numDocsToKeep = 0;
        for (int i = 0, attempts = randomIntBetween(5, 10); i < attempts; i++) {
            final int numExtraDocs = between(10, 100);
            for (long j = 0; j < numExtraDocs; j++) {
                indexDoc(indexShard, "_doc", Long.toString(numDocs + j), "{}");
            }
            numDocs += numExtraDocs;

            if (i < attempts - 1) {
                numDocsToKeep += numExtraDocs;
                flushShard(indexShard, true);
            }

        }

        logger.info("--> indexed {} docs, {} to keep", numDocs, numDocsToKeep);

        writeIndexState();

        final ShardPath shardPath = indexShard.shardPath();
        final Path translogPath = shardPath.getDataPath().resolve(ShardPath.TRANSLOG_FOLDER_NAME);

        final ResolveShardCorruptionCommand command = new ResolveShardCorruptionCommand();
        final MockTerminal t = new MockTerminal();
        final OptionParser parser = command.getParser();

        // Try running it before the shard is closed, it should flip out because it can't acquire the lock
        try {
            final OptionSet options = parser.parse("-d", translogPath.toString());
            command.execute(t, options, environment);
            fail("expected the command to fail not being able to acquire the lock");
        } catch (Exception e) {
            assertThat(e.getMessage(), containsString("Failed to lock shard's directory"));
        }

        // close shard
        closeShards(indexShard);

        // TODO: translog does not creates corrupted marker
        // translog is clean - run it before the shard is corrupted
        try {
            final OptionSet options = parser.parse("-d", translogPath.toString());
            command.execute(t, options, environment);
            fail("expected the command to fail as translog is clean");
        } catch (Exception e) {
            assertThat(e.getMessage(),
                allOf(startsWith("Both Lucene index and traslog at"), endsWith(" are clean.")));
            assertThat(t.getOutput(), containsString("Translog is clean at"));
        }

        TestTranslog.corruptTranslogFiles(logger, random(), Arrays.asList(translogPath));

        // open shard with the same location
        final ShardRouting shardRouting = ShardRoutingHelper.initWithSameId(indexShard.routingEntry(),
            RecoverySource.StoreRecoverySource.EXISTING_STORE_INSTANCE
        );

        final IndexMetaData indexMetaData =
            IndexMetaData.builder(indexShard.indexSettings().getIndexMetaData())
                .build();

        final IndexShard corruptedShard = newShard(shardRouting, shardPath, indexMetaData,
            indexSettings -> {
                final ShardId shardId = shardPath.getShardId();
                final DirectoryService directoryService = new DirectoryService(shardId, indexSettings) {
                    @Override
                    public Directory newDirectory() throws IOException {
                        final BaseDirectoryWrapper baseDirectoryWrapper = newFSDirectory(shardPath.resolveIndex());
                        // index is corrupted - don't even try to check index on close - it fails
                        baseDirectoryWrapper.setCheckIndexOnClose(false);
                        return baseDirectoryWrapper;
                    }
                };
                return new Store(shardId, indexSettings, directoryService, new DummyShardLock(shardId));
            },
            null, engineFactory, globalCheckpointSyncer, EMPTY_EVENT_LISTENER);

        // it has to fail on start up due to index.shard.check_on_startup = checksum
        final Exception exception = expectThrows(Exception.class, () -> newStartedShard(p -> corruptedShard, true));
        final Throwable cause = (exception instanceof IndexShardRecoveryException ? exception.getCause() : exception.getCause().getCause());
        assertThat(cause, instanceOf(TranslogCorruptedException.class));

        closeShards(corruptedShard);

        // checking that lock has been released

        try (Directory dir = FSDirectory.open(translogPath, NativeFSLockFactory.INSTANCE);
             Lock writeLock = dir.obtainLock(IndexWriter.WRITE_LOCK_NAME)) {
            // Great, do nothing, we just wanted to obtain the lock
        }

        // index is closed, lock is released - run command with dry-run
        {
            t.addTextInput("n"); // mean dry run
            final OptionSet options = parser.parse("-d", translogPath.toString());
            t.setVerbosity(Terminal.Verbosity.VERBOSE);
            try {
                command.execute(t, options, environment);
                fail();
            } catch (ElasticsearchException e) {
                assertThat(e.getMessage(), containsString("aborted by user"));
                assertThat(t.getOutput(), containsString("Continue and DELETE files?"));
            }

            logger.info("--> output:\n{}", t.getOutput());
        }

        // index is closed, lock is released - run command
        t.addTextInput("y");
        final OptionSet options = parser.parse("-d", translogPath.toString());

        command.execute(t, options, environment);

        final String output = t.getOutput();
        logger.info("--> output:\n{}", output);

        // reopen shard, do checksum on start up

        final IndexShard newShard = newStartedShard(p ->
                newShard(shardRouting, shardPath, indexMetaData,
                    null, null, engineFactory, globalCheckpointSyncer, EMPTY_EVENT_LISTENER),
            true);

        final Set<String> shardDocUIDs = getShardDocUIDs(newShard);

        assertThat(shardDocUIDs.size(), equalTo(numDocsToKeep));

        closeShards(newShard);
    }

    public void testResolveIndexDirectory() throws Exception {
        final IndexShard indexShard = newStartedShard(p ->
                newShard(routing, shardPath, indexMetaData, null, null,
                    new InternalEngineFactory(), () -> {}, EMPTY_EVENT_LISTENER),
            true);

        // index single doc to have files on disk
        indexDoc(indexShard, "_doc", "0", "{}");
        flushShard(indexShard, true);
        writeIndexState();

        final Path indexPath = shardPath.getDataPath().resolve(ShardPath.INDEX_FOLDER_NAME);

        // close shard
        closeShards(indexShard);

        final ResolveShardCorruptionCommand command = new ResolveShardCorruptionCommand();
        final OptionParser parser = command.getParser();

        // `--index index_name --shard-id 0` has to be resolved to indexPath
        final OptionSet options = parser.parse("--index", shardId.getIndex().getName(),
            "--shard-id", Integer.toString(shardId.id()));
        final ShardPath shardPath = command.getShardPath(options, environment);

        assertThat(shardPath.resolveIndex(), equalTo(indexPath));

    }

    static void corruptIndexFiles(Path indexPath, boolean corruptSegments) throws IOException {
        // corrupt files
        final Path[] filesToCorrupt =
            Files.walk(indexPath)
                .filter(p -> Files.isRegularFile(p) && IndexWriter.WRITE_LOCK_NAME.equals(p.getFileName().toString()) == false)
                .filter(p -> {
                    final String name = p.getFileName().toString();
                    final boolean segmentFile = name.startsWith("segments_") || name.endsWith(".si");
                    return corruptSegments ? segmentFile : segmentFile == false;
                })
                .toArray(Path[]::new);
        CorruptionUtils.corruptFile(random(), filesToCorrupt);
    }

    private void writeIndexState() throws IOException {
        // create _state of IndexMetaData
        try(NodeEnvironment nodeEnvironment = new NodeEnvironment(environment.settings(), environment)) {
            final Path[] paths = nodeEnvironment.indexPaths(indexMetaData.getIndex());
            IndexMetaData.FORMAT.write(indexMetaData, paths);
            logger.info("--> index metadata persisted to {} ", Arrays.toString(paths));
        }
    }

}
