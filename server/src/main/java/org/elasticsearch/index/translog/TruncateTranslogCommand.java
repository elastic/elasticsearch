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

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.NativeFSLockFactory;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.shard.ShardToolCommand;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TruncateTranslogCommand extends ShardToolCommand {

    private final OptionSpec<Void> batchMode;

    public TruncateTranslogCommand() {
        super("Truncates a translog to create a new, empty translog", "Translog");
        this.batchMode = parser.acceptsAll(Arrays.asList("b", "batch"),
                "Enable batch mode explicitly, automatic confirmation of warnings");
    }

    @Override
    protected void printAdditionalHelp(Terminal terminal) {
        terminal.println("This tool truncates the translog");
        terminal.println("checkpoint files to create a new translog");
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        warnAboutESShouldBeStopped(terminal);

        boolean batch = options.has(batchMode);

        final ShardPath shardPath = getShardPath(options, env);

        final Path indexPath = shardPath.resolveIndex();
        final Path translogPath = shardPath.resolveTranslog();
        if (Files.exists(translogPath) == false || Files.isDirectory(translogPath) == false) {
            throw new ElasticsearchException("translog directory [" + translogPath + "], must exist and be a directory");
        }

        try (Directory dir = FSDirectory.open(indexPath, NativeFSLockFactory.INSTANCE)) {
            final String historyUUID = UUIDs.randomBase64UUID();
            final Map<String, String> commitData;
            // Hold the lock open for the duration of the tool running
            try (Lock writeLock = dir.obtainLock(IndexWriter.WRITE_LOCK_NAME)) {
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
                    commits = DirectoryReader.listCommits(dir);
                } catch (IndexNotFoundException infe) {
                    throw new ElasticsearchException("unable to find a valid shard at [" + indexPath + "]", infe);
                }

                // Retrieve the generation and UUID from the existing data
                commitData = new HashMap<>(commits.get(commits.size() - 1).getUserData());
                String translogGeneration = commitData.get(Translog.TRANSLOG_GENERATION_KEY);
                String translogUUID = commitData.get(Translog.TRANSLOG_UUID_KEY);

                final long globalCheckpoint;
                // In order to have a safe commit invariant, we have to assign the global checkpoint to the max_seqno of the last commit.
                // We can only safely do it because we will generate a new history uuid this shard.
                if (commitData.containsKey(SequenceNumbers.MAX_SEQ_NO)) {
                    globalCheckpoint = Long.parseLong(commitData.get(SequenceNumbers.MAX_SEQ_NO));
                    // Also advances the local checkpoint of the last commit to its max_seqno.
                    commitData.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(globalCheckpoint));
                } else {
                    globalCheckpoint = SequenceNumbers.UNASSIGNED_SEQ_NO;
                }
                if (translogGeneration == null || translogUUID == null) {
                    throw new ElasticsearchException("shard must have a valid translog generation and UUID but got: [{}] and: [{}]",
                        translogGeneration, translogUUID);
                }

                final boolean clean = isTranslogClean(shardPath, translogGeneration, translogUUID);

                // Warn about ES being stopped and files being deleted
                warnAboutDeletingFiles(terminal, translogFiles, batch, clean);

                terminal.println("Translog Generation: " + translogGeneration);
                terminal.println("Translog UUID      : " + translogUUID);
                terminal.println("History UUID      : " + historyUUID);

                Path tempEmptyCheckpoint = translogPath.resolve("temp-" + Translog.CHECKPOINT_FILE_NAME);
                Path realEmptyCheckpoint = translogPath.resolve(Translog.CHECKPOINT_FILE_NAME);
                Path tempEmptyTranslog = translogPath.resolve("temp-" + Translog.TRANSLOG_FILE_PREFIX +
                    translogGeneration + Translog.TRANSLOG_FILE_SUFFIX);
                Path realEmptyTranslog = translogPath.resolve(Translog.TRANSLOG_FILE_PREFIX +
                    translogGeneration + Translog.TRANSLOG_FILE_SUFFIX);

                // Write empty checkpoint and translog to empty files
                long gen = Long.parseLong(translogGeneration);
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

            addNewHistoryCommit(dir, terminal, commitData);
            newAllocationId(env, shardPath, terminal);
        } catch (LockObtainFailedException lofe) {
            throw new ElasticsearchException("Failed to lock shard's directory at [" + indexPath + "], is Elasticsearch still running?");
        }

        terminal.println("Done.");
    }

    private boolean isTranslogClean(ShardPath shardPath, String translogGeneration, String translogUUID) throws IOException {
        // TODO: perform clean check of translog instead of corrupted marker file
        boolean clean = true;
        try {
            final Path translogPath = shardPath.resolveTranslog();
            final long translogGlobalCheckpoint = Translog.readGlobalCheckpoint(translogPath, translogUUID);
            final IndexMetaData indexMetaData =
                IndexMetaData.FORMAT.loadLatestState(logger, NamedXContentRegistry.EMPTY, shardPath.getDataPath().getParent());
            final IndexSettings indexSettings = new IndexSettings(indexMetaData, Settings.EMPTY);
            final TranslogConfig translogConfig = new TranslogConfig(shardPath.getShardId(), translogPath,
                indexSettings, BigArrays.NON_RECYCLING_INSTANCE);
            long primaryTerm = Long.MAX_VALUE;
            final TranslogDeletionPolicy translogDeletionPolicy =
                new TranslogDeletionPolicy(indexSettings.getTranslogRetentionSize().getBytes(),
                    indexSettings.getTranslogRetentionAge().getMillis());
            try (Translog translog = new Translog(translogConfig, translogUUID,
                translogDeletionPolicy, () -> translogGlobalCheckpoint, () -> primaryTerm);
                 Translog.Snapshot snapshot = translog.newSnapshotFromGen(Long.parseLong(translogGeneration))) {
                while (snapshot.next() != null) {
                    // just iterate over snapshot
                }
            }
        } catch (TranslogCorruptedException e) {
            clean = false;
        }
        return clean;
    }

    /** Write a checkpoint file to the given location with the given generation */
    static void writeEmptyCheckpoint(Path filename, int translogLength, long translogGeneration, long globalCheckpoint) throws IOException {
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
            TranslogHeader header = new TranslogHeader(translogUUID, TranslogHeader.UNKNOWN_PRIMARY_TERM);
            header.write(fc);
            return header.sizeInBytes();
        }
    }

    /** Show a warning about deleting files, asking for a confirmation if {@code batchMode} is false */
    private void warnAboutDeletingFiles(Terminal terminal, Set<Path> files, boolean batchMode, boolean clean) {
        terminal.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        terminal.println("!   WARNING:    Documents inside of translog files will be lost       !");
        terminal.println("!                                                                     !");
        terminal.println("!   WARNING:          The following files will be DELETED!            !");
        terminal.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        for (Path file : files) {
            terminal.println("--> " + file);
        }
        terminal.println("");

        if (clean) {
            confirm(toolPrefix  + " looks clean. "
                    + "Are you taking a risk of losing documents to DELETE files ?",
                terminal);
        } else if (batchMode == false) {
            confirm("Continue and DELETE files?", terminal);
        }
    }

    /** Return a Set of all files in a given directory */
    public static Set<Path> filesInDirectory(Path directory) throws IOException {
        Set<Path> files = new HashSet<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
            for (Path file : stream) {
                files.add(file);
            }
        }
        return files;
    }

}
