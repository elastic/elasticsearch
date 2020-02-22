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

package org.elasticsearch.index.translog;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.Directory;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.RemoveCorruptedShardDataCommand;
import org.elasticsearch.index.shard.ShardPath;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class TruncateTranslogAction {

    protected static final Logger logger = LogManager.getLogger(TruncateTranslogAction.class);
    private final NamedXContentRegistry namedXContentRegistry;

    public TruncateTranslogAction(NamedXContentRegistry namedXContentRegistry) {
        this.namedXContentRegistry = namedXContentRegistry;
    }

    public Tuple<RemoveCorruptedShardDataCommand.CleanStatus, String> getCleanStatus(ShardPath shardPath,
                                                                                     ClusterState clusterState,
                                                                                     Directory indexDirectory) throws IOException {
        final Path indexPath = shardPath.resolveIndex();
        final Path translogPath = shardPath.resolveTranslog();
        final List<IndexCommit> commits;
        try {
            commits = DirectoryReader.listCommits(indexDirectory);
        } catch (IndexNotFoundException infe) {
            throw new ElasticsearchException("unable to find a valid shard at [" + indexPath + "]", infe);
        } catch (IOException e) {
            throw new ElasticsearchException("unable to list commits at [" + indexPath + "]", e);
        }

        // Retrieve the generation and UUID from the existing data
        final Map<String, String> commitData = new HashMap<>(commits.get(commits.size() - 1).getUserData());
        final String translogUUID = commitData.get(Translog.TRANSLOG_UUID_KEY);

        if (translogUUID == null) {
            throw new ElasticsearchException("shard must have a valid translog UUID but got: [null]");
        }

        final boolean clean = isTranslogClean(shardPath, clusterState, translogUUID);

        if (clean) {
            return Tuple.tuple(RemoveCorruptedShardDataCommand.CleanStatus.CLEAN, null);
        }

        // Hold the lock open for the duration of the tool running
        Set<Path> translogFiles;
        try {
            translogFiles = filesInDirectory(translogPath);
        } catch (IOException e) {
            throw new ElasticsearchException("failed to find existing translog files", e);
        }
        final String details = deletingFilesDetails(translogPath, translogFiles);

        return Tuple.tuple(RemoveCorruptedShardDataCommand.CleanStatus.CORRUPTED, details);
    }

    public void execute(Terminal terminal, ShardPath shardPath, Directory indexDirectory) throws IOException {
        final Path indexPath = shardPath.resolveIndex();
        final Path translogPath = shardPath.resolveTranslog();

        final String historyUUID = UUIDs.randomBase64UUID();
        final Map<String, String> commitData;
        // Hold the lock open for the duration of the tool running
        Set<Path> translogFiles;
        try {
            terminal.println("Checking existing translog files");
            translogFiles = filesInDirectory(translogPath);
        } catch (IOException e) {
            terminal.println("encountered IOException while listing directory, aborting...");
            throw new ElasticsearchException("failed to find existing translog files", e);
        }

        List<IndexCommit> commits;
        try {
            terminal.println("Reading translog UUID information from Lucene commit from shard at [" + indexPath + "]");
            commits = DirectoryReader.listCommits(indexDirectory);
        } catch (IndexNotFoundException infe) {
            throw new ElasticsearchException("unable to find a valid shard at [" + indexPath + "]", infe);
        }

        // Retrieve the generation and UUID from the existing data
        commitData = commits.get(commits.size() - 1).getUserData();
        final String translogUUID = commitData.get(Translog.TRANSLOG_UUID_KEY);
        if (translogUUID == null) {
            throw new ElasticsearchException("shard must have a valid translog UUID");
        }

        final long globalCheckpoint = commitData.containsKey(SequenceNumbers.MAX_SEQ_NO)
            ? Long.parseLong(commitData.get(SequenceNumbers.MAX_SEQ_NO))
            : SequenceNumbers.UNASSIGNED_SEQ_NO;

        terminal.println("Translog UUID      : " + translogUUID);
        terminal.println("History UUID       : " + historyUUID);

        Path tempEmptyCheckpoint = translogPath.resolve("temp-" + Translog.CHECKPOINT_FILE_NAME);
        Path realEmptyCheckpoint = translogPath.resolve(Translog.CHECKPOINT_FILE_NAME);
        final long gen = 1;
        Path tempEmptyTranslog = translogPath.resolve("temp-" + Translog.TRANSLOG_FILE_PREFIX + gen + Translog.TRANSLOG_FILE_SUFFIX);
        Path realEmptyTranslog = translogPath.resolve(Translog.TRANSLOG_FILE_PREFIX + gen + Translog.TRANSLOG_FILE_SUFFIX);

        // Write empty checkpoint and translog to empty files
        int translogLen = writeEmptyTranslog(tempEmptyTranslog, translogUUID);
        writeEmptyCheckpoint(tempEmptyCheckpoint, translogLen, gen, globalCheckpoint);

        terminal.println("Removing existing translog files");
        IOUtils.rm(translogFiles.toArray(new Path[]{}));

        terminal.println("Creating new empty checkpoint at [" + realEmptyCheckpoint + "]");
        Files.move(tempEmptyCheckpoint, realEmptyCheckpoint, StandardCopyOption.ATOMIC_MOVE);
        terminal.println("Creating new empty translog at [" + realEmptyTranslog + "]");
        Files.move(tempEmptyTranslog, realEmptyTranslog, StandardCopyOption.ATOMIC_MOVE);

        // Fsync the translog directory after rename
        IOUtils.fsync(translogPath, true);
    }

    private boolean isTranslogClean(ShardPath shardPath, ClusterState clusterState, String translogUUID) throws IOException {
        // perform clean check of translog instead of corrupted marker file
        try {
            final Path translogPath = shardPath.resolveTranslog();
            final long translogGlobalCheckpoint = Translog.readGlobalCheckpoint(translogPath, translogUUID);
            final IndexMetaData indexMetaData = clusterState.metaData().getIndexSafe(shardPath.getShardId().getIndex());
            final IndexSettings indexSettings = new IndexSettings(indexMetaData, Settings.EMPTY);
            final TranslogConfig translogConfig = new TranslogConfig(shardPath.getShardId(), translogPath,
                indexSettings, BigArrays.NON_RECYCLING_INSTANCE);
            long primaryTerm = indexSettings.getIndexMetaData().primaryTerm(shardPath.getShardId().id());
            final TranslogDeletionPolicy translogDeletionPolicy = new TranslogDeletionPolicy();
            try (Translog translog = new Translog(translogConfig, translogUUID,
                translogDeletionPolicy, () -> translogGlobalCheckpoint, () -> primaryTerm, seqNo -> {});
                 Translog.Snapshot snapshot = translog.newSnapshot(0, Long.MAX_VALUE)) {
                //noinspection StatementWithEmptyBody we are just checking that we can iterate through the whole snapshot
                while (snapshot.next() != null) {
                }
            }
            return true;
        } catch (TranslogCorruptedException e) {
            return false;
        }
    }

    /** Write a checkpoint file to the given location with the given generation */
    private static void writeEmptyCheckpoint(Path filename, int translogLength, long translogGeneration, long globalCheckpoint)
            throws IOException {
        Checkpoint emptyCheckpoint = Checkpoint.emptyTranslogCheckpoint(translogLength, translogGeneration,
            globalCheckpoint, translogGeneration);
        Checkpoint.write(FileChannel::open, filename, emptyCheckpoint,
            StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE_NEW);
        // fsync with metadata here to make sure.
        IOUtils.fsync(filename, false);
    }

    /**
     * Write a translog containing the given translog UUID to the given location. Returns the number of bytes written.
     */
    private static int writeEmptyTranslog(Path filename, String translogUUID) throws IOException {
        try (FileChannel fc = FileChannel.open(filename, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)) {
            TranslogHeader header = new TranslogHeader(translogUUID, SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
            header.write(fc);
            return header.sizeInBytes();
        }
    }

    /** Show a warning about deleting files, asking for a confirmation if {@code batchMode} is false */
    private String deletingFilesDetails(Path translogPath, Set<Path> files) {
        StringBuilder builder = new StringBuilder();

        builder
            .append("Documents inside of translog files will be lost.\n")
            .append("  The following files will be DELETED at ")
            .append(translogPath)
            .append("\n\n");
        for(Iterator<Path> it = files.iterator();it.hasNext();) {
            builder.append("  --> ").append(it.next().getFileName());
            if (it.hasNext()) {
                builder.append("\n");
            }
        }
        return builder.toString();
    }

    /** Return a Set of all files in a given directory */
    private static Set<Path> filesInDirectory(Path directory) throws IOException {
        Set<Path> files = new TreeSet<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
            for (Path file : stream) {
                files.add(file);
            }
        }
        return files;
    }

}
