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
import joptsimple.OptionSpec;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.NativeFSLockFactory;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cli.EnvironmentAwareCommand;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.allocation.command.AllocateEmptyPrimaryAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocateStalePrimaryAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.NodeMetaData;
import org.elasticsearch.gateway.MetaDataStateFormat;
import org.elasticsearch.http.HttpTransportSettings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.TruncateTranslogAction;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

public class RemoveCorruptedShardSegmentsCommand extends EnvironmentAwareCommand {

    private static final Logger logger = Loggers.getLogger(RemoveCorruptedShardSegmentsCommand.class);

    private static final int MISCONFIGURATION = 1;

    private final OptionSpec<String> folderOption;
    private final OptionSpec<String> indexNameOption;
    private final OptionSpec<Integer> shardIdOption;
    private final OptionSpec<String> dryRunOption;

    private final RemoveCorruptedLuceneSegmentsAction removeCorruptedLuceneSegmentsAction;
    private final TruncateTranslogAction truncateTranslogAction;

    public RemoveCorruptedShardSegmentsCommand() {
        super("Removes corrupted shard files");

        folderOption = parser.acceptsAll(Arrays.asList("d", "dir"),
            "Index directory location on disk")
            .withRequiredArg();

        indexNameOption = parser.accepts("index", "Index name")
            .withRequiredArg();

        shardIdOption = parser.accepts("shard-id", "Shard id")
            .withRequiredArg()
            .ofType(Integer.class);

        dryRunOption = parser.accepts("dry-run", "Only perform analysis")
            .withOptionalArg();

        removeCorruptedLuceneSegmentsAction = new RemoveCorruptedLuceneSegmentsAction();
        truncateTranslogAction = new TruncateTranslogAction();
    }

    @Override
    protected void printAdditionalHelp(Terminal terminal) {
        terminal.println("This tool removes the corrupted Lucene segments and/or truncates the translog");
    }

    // Visible for testing
    public OptionParser getParser() {
        return this.parser;
    }

    @SuppressForbidden(reason = "Necessary to use the path passed in")
    protected Path getPath(String dirValue) {
        return PathUtils.get(dirValue, "", "");
    }

    protected ShardPath getShardPath(OptionSet options, Environment environment) throws IOException, UserException {
        final Settings settings = environment.settings();
        final String dirValue = folderOption.value(options);
        if (dirValue != null) {
            final Path path = getPath(dirValue).getParent();
            final ShardPath shardPath = resolveShardPath(environment, path);
            return shardPath;
        }

        // otherwise - try to resolve shardPath based on the index name and shard id

        final String indexName = Objects.requireNonNull(indexNameOption.value(options), "Index name is required");
        final Integer id = Objects.requireNonNull(shardIdOption.value(options), "Shard ID is required");

        return resolveShardPath(environment,
            (nodeLockId, nodePath) -> {
                // have to scan all index uuid folders to resolve from index name
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(nodePath.indicesPath)) {
                    for (Path file : stream) {
                        if (Files.exists(file.resolve(MetaDataStateFormat.STATE_DIR_NAME))
                            // and shard id folder
                            && Files.exists(file.resolve(Integer.toString(id)))) {
                            final IndexMetaData indexMetaData =
                                IndexMetaData.FORMAT.loadLatestState(logger, NamedXContentRegistry.EMPTY, file);
                            if (indexMetaData != null) {
                                final IndexSettings indexSettings = new IndexSettings(indexMetaData, settings);
                                final Index index = indexMetaData.getIndex();
                                if (indexName.equals(index.getName())) {
                                    final ShardId shardId = new ShardId(index, id);

                                    final Path shardPathLocation = nodePath.resolve(shardId);
                                    final ShardPath shardPath = ShardPath.loadShardPath(logger, shardId, indexSettings,
                                        new Path[]{shardPathLocation},
                                        nodeLockId, nodePath.path);
                                    if (shardPath != null) {
                                        return shardPath;
                                    }
                                }
                            }
                        }
                    }
                }
                return null;
            },
            () -> new ElasticsearchException("Unable to resolve shard path for index [" + indexName + "] and shard id [" + id + "]"));
    }

    /**
     * resolve {@code ShardPath} based on a full index or translog path
     * @param environment
     * @param path
     * @return
     * @throws IOException
     * @throws UserException
     */
    private ShardPath resolveShardPath(Environment environment, Path path) throws IOException, UserException {
        final Settings settings = environment.settings();
        final Path shardParentPath = path.getParent();
        final Path indexPath = path.resolve(ShardPath.INDEX_FOLDER_NAME);
        if (Files.exists(indexPath) == false || Files.isDirectory(indexPath) == false) {
            throw new ElasticsearchException("index directory [" + indexPath + "], must exist and be a directory");
        }

        final IndexMetaData indexMetaData =
            IndexMetaData.FORMAT.loadLatestState(logger, NamedXContentRegistry.EMPTY, shardParentPath);
        final Index index = indexMetaData.getIndex();
        final ShardId shardId;
        final String fileName = path.getFileName().toString();
        if (Files.isDirectory(path) && fileName.chars().allMatch(Character::isDigit)) {
            int id = Integer.parseInt(fileName);
            shardId = new ShardId(index, id);
        } else {
            throw new ElasticsearchException("Unable to resolve shard id from " + path.toString());
        }

        final IndexSettings indexSettings = new IndexSettings(indexMetaData, settings);

        return resolveShardPath(environment,
            (nodeLockId, nodePath) -> {
                final Path shardPathLocation = nodePath.resolve(shardId);
                final ShardPath shardPath = ShardPath.loadShardPath(logger, shardId, indexSettings, new Path[]{shardPathLocation},
                    nodeLockId, nodePath.path);
                if (shardPath != null && shardPath.resolveIndex().equals(indexPath)) {
                    return shardPath;
                }
                return null;
            },
            () -> new ElasticsearchException("Unable to resolve shard path for path " + path.toString()));
    }

    private ShardPath resolveShardPath(final Environment environment,
                                       final CheckedBiFunction<Integer, NodeEnvironment.NodePath, ShardPath, IOException> function,
                                       final Supplier<ElasticsearchException> exceptionSupplier)
        throws IOException, UserException {
        // have to iterate over possibleLockId as NodeEnvironment - but fail if node owns lock
        final Settings settings = environment.settings();
        // try to resolve shard path in case of multi-node layout per environment (hope it's only integration tests)
        final int maxLocalStorageNodes = NodeEnvironment.MAX_LOCAL_STORAGE_NODES_SETTING.get(settings);

        nodeLoop:
        for (int possibleLockId = 0; possibleLockId < maxLocalStorageNodes; possibleLockId++) {
            for (int dirIndex = 0; dirIndex < environment.dataFiles().length; dirIndex++) {
                final Path dataDir = environment.dataFiles()[dirIndex];
                final Path dir = NodeEnvironment.resolveNodePath(dataDir, possibleLockId);
                if (Files.exists(dir) == false) {
                    // assume that we do not have gaps in nodes like
                    break nodeLoop;
                }
                try (Directory luceneDir = FSDirectory.open(dir, NativeFSLockFactory.INSTANCE);
                     Lock lock = luceneDir.obtainLock(NodeEnvironment.NODE_LOCK_FILENAME)) {
                }  catch (LockObtainFailedException lofe) {
                    throw new UserException(MISCONFIGURATION,
                        "Failed to lock node's directory at [" + dir + "], is Elasticsearch still running?");
                } catch (IOException e) {
                    throw e;
                }
                final NodeEnvironment.NodePath nodePath = new NodeEnvironment.NodePath(dir);
                if (Files.exists(nodePath.indicesPath) == false) {
                    break;
                }

                final ShardPath shardPath = function.apply(possibleLockId, nodePath);
                if (shardPath != null) {
                    return shardPath;
                }
            }
        }
        throw exceptionSupplier.get();
    }

    public static boolean isCorruptMarkerFileIsPresent(final Directory directory) throws IOException {
        boolean found = false;

        final String[] files = directory.listAll();
        for (String file : files) {
            if (file.startsWith(Store.CORRUPTED)) {
                found = true;
                break;
            }
        }

        return found;
    }

    protected void dropCorruptMarkerFiles(Terminal terminal, Directory directory, String toolPrefix, boolean clean) throws IOException {
        if (clean) {
            confirm(toolPrefix  + " looks clean but a corrupted marker is there - "
                    + "it means that some problem happened. "
                    + "Are you taking a risk of losing documents and proceed with removing a corrupted marker ?",
                terminal);
        }
        String[] files = directory.listAll();
        boolean found = false;
        for (String file : files) {
            if (file.startsWith(Store.CORRUPTED)) {
                directory.deleteFile(file);

                terminal.println("Deleted corrupt marker " + file);
            }
        }
    }

    private static void loseDataBanner(Terminal terminal, Tuple<CleanStatus, String> cleanStatus, boolean dryRun) {
        terminal.println("-----------------------------------------------------------------------");
        terminal.println("");
        if (cleanStatus.v2() != null) {
            terminal.println("  " + cleanStatus.v2());
        }
        if (dryRun == false) {
            terminal.println("");
            terminal.println("            WARNING:              YOU WILL LOSE DATA.                  ");
        }
        terminal.println("-----------------------------------------------------------------------");
    }

    private static void confirm(String msg, Terminal terminal) {
        terminal.println(msg);
        String text = terminal.readText("Confirm [y/N] ");
        if (text.equalsIgnoreCase("y") == false) {
            throw new ElasticsearchException("aborted by user");
        }
    }

    private static void warnAboutESShouldBeStopped(Terminal terminal) {
        terminal.println("-----------------------------------------------------------------------");
        terminal.println("");
        terminal.println("    WARNING: ElasticSearch MUST be stopped before running this tool.");
        terminal.println("");
        terminal.println("  Please make a complete backup of your index before using this tool.");
        terminal.println("");
        terminal.println("-----------------------------------------------------------------------");
    }

    // Visible for testing
    @Override
    public void execute(Terminal terminal, OptionSet options, Environment environment) throws Exception {
        warnAboutESShouldBeStopped(terminal);

        final ShardPath shardPath = getShardPath(options, environment);

        final boolean dryRun = options.has(dryRunOption);

        final Path indexPath = shardPath.resolveIndex();
        final Path translogPath = shardPath.resolveTranslog();
        if (Files.exists(translogPath) == false || Files.isDirectory(translogPath) == false) {
            throw new ElasticsearchException("translog directory [" + translogPath + "], must exist and be a directory");
        }

        final PrintWriter writer = terminal.getWriter();
        final PrintStream printStream = new PrintStream(new OutputStream() {
            @Override
            public void write(int b) {
                writer.write(b);
            }
        }, false, "UTF-8");
        final boolean verbose = terminal.isPrintable(Terminal.Verbosity.VERBOSE);

        Directory directory;
        try {
            directory = FSDirectory.open(indexPath, NativeFSLockFactory.INSTANCE);
        } catch (Throwable t) {
            throw new ElasticsearchException("ERROR: could not open directory \"" + indexPath + "\"; exiting");
        }

        final Tuple<CleanStatus, String> indexCleanStatus;
        final Tuple<CleanStatus, String> translogCleanStatus;
        try (Directory indexDirectory = directory) {
            // Hold the lock open for the duration of the tool running
            try (Lock writeLock = indexDirectory.obtainLock(IndexWriter.WRITE_LOCK_NAME)) {
                ////////// Index
                terminal.println("");
                terminal.println("Opening Lucene index at " + indexPath);
                terminal.println("");
                try {
                    indexCleanStatus = removeCorruptedLuceneSegmentsAction.getCleanStatus(shardPath, indexDirectory,
                        writeLock, printStream, verbose);
                } catch (Exception e) {
                    terminal.println(e.getMessage());
                    throw e;
                }

                terminal.println("");
                terminal.println(" >> Lucene index is " + indexCleanStatus.v1().getMessage() + " at " + indexPath);
                terminal.println("");

                ////////// Translog
                // as translog relies on data stored in an index commit - to truncate translog - we have to have non unrecoverable index
                if (indexCleanStatus.v1() != CleanStatus.UNRECOVERABLE) {
                    terminal.println("");
                    terminal.println("Opening translog at " + translogPath);
                    terminal.println("");
                    try {
                        translogCleanStatus = truncateTranslogAction.getCleanStatus(shardPath, indexDirectory);
                    } catch (Exception e) {
                        terminal.println(e.getMessage());
                        throw e;
                    }

                    terminal.println("");
                    terminal.println(" >> Translog is " + translogCleanStatus.v1().getMessage() + " at " + translogPath);
                    terminal.println("");
                } else {
                    translogCleanStatus = Tuple.tuple(CleanStatus.UNRECOVERABLE, null);
                }

                ////////// Drop corrupted parts
                final CleanStatus indexStatus = indexCleanStatus.v1();
                final CleanStatus translogStatus = translogCleanStatus.v1();
                if (dryRun) {
                    if (indexStatus == CleanStatus.CLEAN && translogStatus == CleanStatus.CLEAN) {
                        terminal.println("Both Lucene index and traslog at " + shardPath.getDataPath() + " are clean.");
                    } else {
                        if (indexStatus != CleanStatus.CLEAN) {
                            loseDataBanner(terminal, indexCleanStatus, dryRun);
                        }

                        if (translogStatus != CleanStatus.CLEAN) {
                            loseDataBanner(terminal, translogCleanStatus, dryRun);
                        }
                    }
                } else {

                    if (indexStatus == CleanStatus.CLEAN && translogStatus == CleanStatus.CLEAN) {
                        throw new ElasticsearchException("Both Lucene index and traslog at " + shardPath.getDataPath() + " are clean.");
                    }

                    if (indexStatus == CleanStatus.UNRECOVERABLE) {
                        if (indexCleanStatus.v2() != null) {
                            terminal.println("Details: " + indexCleanStatus.v2());
                        }

                        terminal.println("You can allocate completely empty primary shard with command: ");

                        printRerouteCommand(environment, shardPath, terminal, true);

                        throw new ElasticsearchException("Index is unrecoverable - there are missing segments");
                    }


                    if (indexStatus != CleanStatus.CLEAN) {
                        loseDataBanner(terminal, indexCleanStatus, dryRun);
                        confirm("Continue and remove docs from the index ?", terminal);
                        removeCorruptedLuceneSegmentsAction.execute(terminal, shardPath, indexDirectory, writeLock, printStream, verbose);
                    }

                    if (translogStatus != CleanStatus.CLEAN) {
                        loseDataBanner(terminal, translogCleanStatus, dryRun);
                        confirm("Continue and DELETE files?", terminal);
                        truncateTranslogAction.execute(terminal, shardPath, indexDirectory);
                    }
                }
            } catch (LockObtainFailedException lofe) {
                final String msg = "Failed to lock shard's directory at [" + indexPath + "], is Elasticsearch still running?";
                terminal.println(msg);
                throw new ElasticsearchException(msg);
            }

            final CleanStatus indexStatus = indexCleanStatus.v1();
            final CleanStatus translogStatus = translogCleanStatus.v1();
            // newHistoryCommit obtains its own lock
            if (dryRun == false && (indexStatus != CleanStatus.CLEAN || translogStatus != CleanStatus.CLEAN)) {
                addNewHistoryCommit(indexDirectory, terminal, translogStatus != CleanStatus.CLEAN);
                newAllocationId(environment, shardPath, terminal);
                if (indexStatus != CleanStatus.CLEAN) {
                    dropCorruptMarkerFiles(terminal, indexDirectory, "Lucene index",
                        indexStatus == CleanStatus.CLEAN_WITH_CORRUPTED_MARKER);
                }
            }
        }
    }

    protected void addNewHistoryCommit(Directory indexDirectory, Terminal terminal, boolean updateLocalCheckpoint) throws IOException {
        final String historyUUID = UUIDs.randomBase64UUID();

        terminal.println("Marking index with the new history uuid : " + historyUUID);
        // commit the new history id
        final IndexWriterConfig iwc = new IndexWriterConfig(null)
            // we don't want merges to happen here - we call maybe merge on the engine
            // later once we stared it up otherwise we would need to wait for it here
            // we also don't specify a codec here and merges should use the engines for this index
            .setCommitOnClose(false)
            .setMergePolicy(NoMergePolicy.INSTANCE)
            .setOpenMode(IndexWriterConfig.OpenMode.APPEND);
        // IndexWriter acquires directory lock by its own
        try (IndexWriter indexWriter = new IndexWriter(indexDirectory, iwc)) {
            final Map<String, String> userData = new HashMap<>();
            indexWriter.getLiveCommitData().forEach(e -> userData.put(e.getKey(), e.getValue()));

            // In order to have a safe commit invariant, we have to assign the global checkpoint to the max_seqno of the last commit.
            // We can only safely do it because we will generate a new history uuid this shard.
            if (updateLocalCheckpoint && userData.containsKey(SequenceNumbers.MAX_SEQ_NO)) {
                final long globalCheckpoint = Long.parseLong(userData.get(SequenceNumbers.MAX_SEQ_NO));
                // Also advances the local checkpoint of the last commit to its max_seqno.
                userData.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(globalCheckpoint));
            }

            // commit the new history id
            userData.put(Engine.HISTORY_UUID_KEY, historyUUID);

            indexWriter.setLiveCommitData(userData.entrySet());
            indexWriter.commit();
        }
    }

    protected void newAllocationId(Environment environment, ShardPath shardPath, Terminal terminal) throws IOException {
        final Path shardStatePath = shardPath.getShardStatePath();
        final ShardStateMetaData shardStateMetaData =
            ShardStateMetaData.FORMAT.loadLatestState(logger, NamedXContentRegistry.EMPTY, shardStatePath);

        if (shardStateMetaData == null) {
            throw new ElasticsearchException("No shard state meta data at " + shardStatePath);
        }

        final AllocationId newAllocationId = AllocationId.newInitializing();

        terminal.println("Changing allocation id " + shardStateMetaData.allocationId.getId()
            + " to " + newAllocationId.getId());

        final ShardStateMetaData newShardStateMetaData =
            new ShardStateMetaData(shardStateMetaData.primary, shardStateMetaData.indexUUID, newAllocationId);

        ShardStateMetaData.FORMAT.write(newShardStateMetaData, shardStatePath);

        terminal.println("You should run follow command to apply allocation id changes: ");

        printRerouteCommand(environment, shardPath, terminal, true);
    }

    private void printRerouteCommand(Environment environment, ShardPath shardPath,
                                     Terminal terminal, boolean allocateStale) throws IOException {
        final IndexMetaData indexMetaData =
            IndexMetaData.FORMAT.loadLatestState(logger, NamedXContentRegistry.EMPTY,
                shardPath.getDataPath().getParent());

        final Path nodeStatePath = shardPath.getDataPath().getParent().getParent().getParent();
        final NodeMetaData nodeMetaData =
            NodeMetaData.FORMAT.loadLatestState(logger, NamedXContentRegistry.EMPTY, nodeStatePath);

        if (nodeMetaData == null) {
            throw new ElasticsearchException("No node meta data at " + nodeStatePath);
        }

        final Settings settings = environment.settings();
        final List<String> hosts = HttpTransportSettings.SETTING_HTTP_HOST.get(settings);
        final int[] ports = HttpTransportSettings.SETTING_HTTP_PORT.get(settings).ports();

        final String nodeId = nodeMetaData.nodeId();
        final String index = indexMetaData.getIndex().getName();
        final int id = shardPath.getShardId().id();
        final AllocationCommands commands = new AllocationCommands(
            allocateStale
                ? new AllocateStalePrimaryAllocationCommand(index, id, nodeId, true)
                : new AllocateEmptyPrimaryAllocationCommand(index, id, nodeId, true));

        terminal.println("");
        terminal.println("$ curl -XPOST 'http://"
            + (hosts.isEmpty() ? "localhost" : hosts.get(0)) + ":" + (ports.length == 0 ? 9200 : ports[0])
            + "/_cluster/reroute' -d '\n"
            + Strings.toString(commands, true, true) + "'");
    }

    public enum CleanStatus {
        CLEAN("clean"),
        CLEAN_WITH_CORRUPTED_MARKER("clean, but there is corrupted marker"),
        CORRUPTED("corrupted"),
        UNRECOVERABLE("corrupted but unrecoverable");

        private final String msg;

        CleanStatus(String msg) {
            this.msg = msg;
        }

        public String getMessage() {
            return msg;
        }
    }

}
