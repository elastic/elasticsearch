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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;

/**
 * Helpers for testing translog.
 */
public class TestTranslog {
    static final Pattern TRANSLOG_FILE_PATTERN = Pattern.compile("translog-(\\d+)\\.tlog");

    public static void corruptRandomTranslogFile(Logger logger, Random random, Collection<Path> translogDirs) throws IOException {
        for (Path translogDir : translogDirs) {
            final long minTranslogGen = minTranslogGenUsedInRecovery(translogDir);
            corruptRandomTranslogFile(logger, random, translogDir, minTranslogGen);
        }
    }

    /**
     * Corrupts random translog file (translog-N.tlog) from the given translog directory.
     *
     * @return a translog file which has been corrupted.
     */
    public static Path corruptRandomTranslogFile(Logger logger, Random random, Path translogDir, long minGeneration) throws IOException {
        Set<Path> candidates = new TreeSet<>(); // TreeSet makes sure iteration order is deterministic
        logger.info("--> Translog dir [{}], minUsedTranslogGen [{}]", translogDir, minGeneration);
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(translogDir)) {
            for (Path item : stream) {
                if (Files.isRegularFile(item)) {
                    final Matcher matcher = TRANSLOG_FILE_PATTERN.matcher(item.getFileName().toString());
                    if (matcher.matches() && Long.parseLong(matcher.group(1)) >= minGeneration) {
                        candidates.add(item);
                    }
                }
            }
        }
        assertThat(candidates, is(not(empty())));

        Path corruptedFile = RandomPicks.randomFrom(random, candidates);
        corruptFile(logger, random, corruptedFile);
        return corruptedFile;
    }


     static void corruptFile(Logger logger, Random random, Path fileToCorrupt) throws IOException {
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
    }

    /**
     * Lists all existing commits in a given index path, then read the minimum translog generation that will be used in recoverFromTranslog.
     */
    public static long minTranslogGenUsedInRecovery(Path translogPath) throws IOException {
        try (NIOFSDirectory directory = new NIOFSDirectory(translogPath.getParent().resolve("index"))) {
            List<IndexCommit> commits = DirectoryReader.listCommits(directory);
            final String translogUUID = commits.get(commits.size() - 1).getUserData().get(Translog.TRANSLOG_UUID_KEY);
            long globalCheckpoint = Translog.readGlobalCheckpoint(translogPath, translogUUID);
            IndexCommit recoveringCommit = CombinedDeletionPolicy.findSafeCommitPoint(commits, globalCheckpoint);
            return Long.parseLong(recoveringCommit.getUserData().get(Translog.TRANSLOG_GENERATION_KEY));
        }
    }

    /**
     * Returns the primary term associated with the current translog writer of the given translog.
     */
    public static long getCurrentTerm(Translog translog) {
        return translog.getCurrent().getPrimaryTerm();
    }

    public static List<Translog.Operation> drainSnapshot(Translog.Snapshot snapshot, boolean sortBySeqNo) throws IOException {
        final List<Translog.Operation> ops = new ArrayList<>(snapshot.totalOperations());
        Translog.Operation op;
        while ((op = snapshot.next()) != null) {
            ops.add(op);
        }
        if (sortBySeqNo) {
            ops.sort(Comparator.comparing(Translog.Operation::seqNo));
        }
        return ops;
    }
}
