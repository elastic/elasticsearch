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
import org.elasticsearch.cluster.routing.allocation.command.AllocateStalePrimaryAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.NodeMetaData;
import org.elasticsearch.http.HttpTransportSettings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.store.Store;

import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public abstract class ShardToolCommand extends EnvironmentAwareCommand {

    protected final Logger logger = Loggers.getLogger(getClass());

    protected static final int MISCONFIGURATION = 1;
    protected static final int FAILURE = 2;

    protected final OptionSpec<String> folderOption;
    protected final OptionSpec<String> indexNameOption;
    protected final OptionSpec<Integer> shardIdOption;
    protected final String toolPrefix;

    public ShardToolCommand(String description, String toolPrefix) {
        super(description);
        this.toolPrefix = toolPrefix;

        folderOption = parser.acceptsAll(Arrays.asList("d", "dir"),
            "Index directory location on disk")
            .withRequiredArg();

        indexNameOption = parser.accepts("index", "Index name")
            .withRequiredArg();

        shardIdOption = parser.accepts("shard-id", "Shard id")
            .withRequiredArg()
            .ofType(Integer.class);
    }

    protected static void warnAboutESShouldBeStopped(Terminal terminal) {
        terminal.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        terminal.println("!                                                                     !");
        terminal.println("!   WARNING: Elasticsearch MUST be stopped before running this tool   !");
        terminal.println("! Please make a complete backup of your index before using this tool. !");
        terminal.println("!                                                                     !");
        terminal.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
    }

    protected void addNewHistoryCommit(Directory dir,
                                       Terminal terminal,
                                       @Nullable Map<String, String> commitData) throws IOException {
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
        try (IndexWriter indexWriter = new IndexWriter(dir, iwc)) {
            final Map<String, String> userData = new HashMap<>();
            if (commitData != null) {
                userData.putAll(commitData);
            } else {
                indexWriter.getLiveCommitData().forEach(e -> userData.put(e.getKey(), e.getValue()));
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
        final AllocationCommands commands = new AllocationCommands(
            new AllocateStalePrimaryAllocationCommand(indexMetaData.getIndex().getName(),
                shardPath.getShardId().id(), nodeId, true));

        terminal.println("You should run follow command to apply allocation id changes: ");
        terminal.println("");
        terminal.println("$ curl -XPOST 'http://"
            + (hosts.isEmpty() ? "localhost" : hosts.get(0)) + ":" + (ports.length == 0 ? 9200 : ports[0])
            + "/_cluster/reroute' -d '\n"
            + Strings.toString(commands, true, true) + "'");
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

        final List<ShardPath> shardPaths = new ArrayList<>();

        resolveShardPath(environment, (nodeLockId, nodePath) -> {
            final SimpleFileVisitor<Path> visitor = new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
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
                                shardPaths.add(shardPath);
                            }
                        }
                    }
                    return FileVisitResult.CONTINUE;
                }
            };
            Files.walkFileTree(nodePath.indicesPath, EnumSet.noneOf(FileVisitOption.class), 1, visitor);
            return shardPaths.isEmpty() == false ? null : Boolean.FALSE;
        });

        if (shardPaths.size() == 1) {
            return shardPaths.get(0);
        }
        throw new ElasticsearchException("Unable to resolve shard path for index [" + indexName + "] and shard id [" + id + "]");
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

        AtomicReference<ShardPath> reference = new AtomicReference<>();
        resolveShardPath(environment, (nodeLockId, nodePath) -> {
            final Path shardPathLocation = nodePath.resolve(shardId);
            final ShardPath shardPath = ShardPath.loadShardPath(logger, shardId, indexSettings, new Path[]{shardPathLocation},
                nodeLockId, nodePath.path);
            if (shardPath == null) {
                return true;
            }
            if (shardPath.resolveIndex().equals(indexPath)) {
                reference.set(shardPath);
                return null;
            }
            return false;
        });
        if (reference.get() != null) {
            return reference.get();
        }
        throw new ElasticsearchException("Unable to resolve shard path for path " + path.toString());
    }

    private void resolveShardPath(Environment environment,
                                  CheckedBiFunction<Integer, NodeEnvironment.NodePath, Boolean, IOException> function)
        throws IOException, UserException {
        // have to iterate over possibleLockId as NodeEnvironment - but fail if node owns lock
        final Settings settings = environment.settings();
        // try to resolve shard path in case of multi-node layout per environment (hope it's only integration tests)
        int maxLocalStorageNodes = NodeEnvironment.MAX_LOCAL_STORAGE_NODES_SETTING.get(settings);
        for (int possibleLockId = 0; possibleLockId < maxLocalStorageNodes; possibleLockId++) {
            for (int dirIndex = 0; dirIndex < environment.dataFiles().length; dirIndex++) {
                Path dataDir = environment.dataFiles()[dirIndex];
                Path dir = NodeEnvironment.resolveNodePath(dataDir, possibleLockId);
                if (Files.exists(dir) == false) {
                    break;
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

                final Boolean apply = function.apply(possibleLockId, nodePath);
                if (apply == null) {
                    return;
                }
                if (apply.booleanValue()) {
                    break;
                }
            }
        }
    }

    protected boolean isCorruptMarkerFileIsPresent(Directory directory) throws IOException {
        boolean found = false;
        String[] files = directory.listAll();
        for (String file : files) {
            if (file.startsWith(Store.CORRUPTED)) {
                found = true;
                break;
            }
        }

        return found;
    }

    protected void dropCorruptMarkerFiles(Terminal terminal, Directory directory, boolean clean) throws IOException {
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

    protected static void confirm(String msg, Terminal terminal) {
        terminal.println(msg);
        String text = terminal.readText("Confirm [y/N] ");
        if (text.equalsIgnoreCase("y") == false) {
            throw new ElasticsearchException("aborted by user");
        }
    }

}
