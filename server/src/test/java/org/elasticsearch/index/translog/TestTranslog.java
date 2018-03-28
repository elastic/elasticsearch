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

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.NIOFSDirectory;
import org.elasticsearch.index.engine.CombinedDeletionPolicy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;

/**
 * Helpers for testing translog.
 */
public class TestTranslog {
    static final Pattern TRANSLOG_FILE_PATTERN = Pattern.compile("translog-(\\d+)\\.tlog");

    /**
     * Corrupts some translog files (translog-N.tlog) from the given translog directories.
     *
     * @return a collection of tlog files that have been corrupted.
     */
    public static Set<Path> corruptTranslogFiles(Logger logger, Random random, Collection<Path> translogDirs) throws IOException {
        Set<Path> candidates = new TreeSet<>(); // TreeSet makes sure iteration order is deterministic
        for (Path translogDir : translogDirs) {
            if (Files.isDirectory(translogDir)) {
                final long minUsedTranslogGen = minTranslogGenUsedInRecovery(translogDir);
                logger.info("--> Translog dir [{}], minUsedTranslogGen [{}]", translogDir, minUsedTranslogGen);
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(translogDir)) {
                    for (Path item : stream) {
                        if (Files.isRegularFile(item)) {
                            // Makes sure that we will corrupt tlog files that are referenced by the Checkpoint.
                            final Matcher matcher = TRANSLOG_FILE_PATTERN.matcher(item.getFileName().toString());
                            if (matcher.matches() && Long.parseLong(matcher.group(1)) >= minUsedTranslogGen) {
                                candidates.add(item);
                            }
                        }
                    }
                }
            }
        }

        Set<Path> corruptedFiles = new HashSet<>();
        if (!candidates.isEmpty()) {
            int corruptions = RandomNumbers.randomIntBetween(random, 5, 20);
            for (int i = 0; i < corruptions; i++) {
                Path fileToCorrupt = RandomPicks.randomFrom(random, candidates);
                try (FileChannel raf = FileChannel.open(fileToCorrupt, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                    // read
                    raf.position(RandomNumbers.randomLongBetween(random, 0, raf.size() - 1));
                    long filePointer = raf.position();
                    ByteBuffer bb = ByteBuffer.wrap(new byte[1]);
                    raf.read(bb);
                    bb.flip();

                    // corrupt
                    byte oldValue = bb.get(0);
                    byte newValue = (byte) (oldValue + 1);
                    bb.put(0, newValue);

                    // rewrite
                    raf.position(filePointer);
                    raf.write(bb);
                    logger.info("--> corrupting file {} --  flipping at position {} from {} to {} file: {}",
                        fileToCorrupt, filePointer, Integer.toHexString(oldValue),
                        Integer.toHexString(newValue), fileToCorrupt);
                }
                corruptedFiles.add(fileToCorrupt);
            }
        }
        assertThat("no translog file corrupted", corruptedFiles, not(empty()));
        return corruptedFiles;
    }

    /**
     * Lists all existing commits in a given index path, then read the minimum translog generation that will be used in recoverFromTranslog.
     */
    private static long minTranslogGenUsedInRecovery(Path translogPath) throws IOException {
        try (NIOFSDirectory directory = new NIOFSDirectory(translogPath.getParent().resolve("index"))) {
            List<IndexCommit> commits = DirectoryReader.listCommits(directory);
            final String translogUUID = commits.get(commits.size() - 1).getUserData().get(Translog.TRANSLOG_UUID_KEY);
            long globalCheckpoint = Translog.readGlobalCheckpoint(translogPath, translogUUID);
            IndexCommit recoveringCommit = CombinedDeletionPolicy.findSafeCommitPoint(commits, globalCheckpoint);
            return Long.parseLong(recoveringCommit.getUserData().get(Translog.TRANSLOG_GENERATION_KEY));
        }
    }
}
