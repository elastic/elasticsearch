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
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.core.internal.io.IOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.index.translog.Translog.CHECKPOINT_FILE_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;

/**
 * Helpers for testing translog.
 */
public class TestTranslog {
    private static final Pattern TRANSLOG_FILE_PATTERN = Pattern.compile("^translog-(\\d+)\\.(tlog|ckp)$");

    /**
     * Corrupts random translog file (translog-N.tlog or translog-N.ckp or translog.ckp) from the given translog directory, ignoring
     * translogs and checkpoints with generations below the generation recorded in the latest index commit found in translogDir/../index/,
     * or writes a corrupted translog-N.ckp file as if from a crash while rolling a generation.
     *
     * <p>
     * See {@link TestTranslog#corruptFile(Logger, Random, Path, boolean)} for details of the corruption applied.
     */
    public static void corruptRandomTranslogFile(Logger logger, Random random, Path translogDir) throws IOException {
        corruptRandomTranslogFile(logger, random, translogDir, Translog.readCheckpoint(translogDir).minTranslogGeneration);
    }

    /**
     * Corrupts random translog file (translog-N.tlog or translog-N.ckp or translog.ckp) from the given translog directory, or writes a
     * corrupted translog-N.ckp file as if from a crash while rolling a generation.
     * <p>
     * See {@link TestTranslog#corruptFile(Logger, Random, Path, boolean)} for details of the corruption applied.
     *
     * @param minGeneration the minimum generation (N) to corrupt. Translogs and checkpoints with lower generation numbers are ignored.
     */
    static void corruptRandomTranslogFile(Logger logger, Random random, Path translogDir, long minGeneration) throws IOException {
        logger.info("--> corruptRandomTranslogFile: translogDir [{}], minUsedTranslogGen [{}]", translogDir, minGeneration);

        Path unnecessaryCheckpointCopyPath = null;
        try {
            final Path checkpointPath = translogDir.resolve(CHECKPOINT_FILE_NAME);
            final Checkpoint checkpoint = Checkpoint.read(checkpointPath);
            unnecessaryCheckpointCopyPath = translogDir.resolve(Translog.getCommitCheckpointFileName(checkpoint.generation));
            if (LuceneTestCase.rarely(random) && Files.exists(unnecessaryCheckpointCopyPath) == false) {
                // if we crashed while rolling a generation then we might have copied `translog.ckp` to its numbered generation file but
                // have not yet written a new `translog.ckp`. During recovery we must also verify that this file is intact, so it's ok to
                // corrupt this file too (either by writing the wrong information, correctly formatted, or by properly corrupting it)
                final Checkpoint checkpointCopy;
                if (LuceneTestCase.usually(random)) {
                    checkpointCopy = checkpoint;
                } else {
                    long newTranslogGeneration = checkpoint.generation + random.nextInt(2);
                    long newMinTranslogGeneration = Math.min(newTranslogGeneration, checkpoint.minTranslogGeneration + random.nextInt(2));
                    long newMaxSeqNo = checkpoint.maxSeqNo + random.nextInt(2);
                    long newMinSeqNo = Math.min(newMaxSeqNo, checkpoint.minSeqNo + random.nextInt(2));
                    long newTrimmedAboveSeqNo = Math.min(newMaxSeqNo, checkpoint.trimmedAboveSeqNo + random.nextInt(2));

                    checkpointCopy = new Checkpoint(checkpoint.offset + random.nextInt(2), checkpoint.numOps + random.nextInt(2),
                        newTranslogGeneration, newMinSeqNo,
                        newMaxSeqNo, checkpoint.globalCheckpoint + random.nextInt(2),
                        newMinTranslogGeneration, newTrimmedAboveSeqNo);
                }
                Checkpoint.write(FileChannel::open, unnecessaryCheckpointCopyPath, checkpointCopy,
                    StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);

                if (checkpointCopy.equals(checkpoint) == false) {
                    logger.info("corruptRandomTranslogFile: created [{}] containing [{}] instead of [{}]", unnecessaryCheckpointCopyPath,
                        checkpointCopy, checkpoint);
                    return;
                } // else checkpoint copy has the correct content so it's now a candidate for the usual kinds of corruption
            }
        } catch (TranslogCorruptedException e) {
            // missing or corrupt checkpoint already, find something else to break...
        }

        Set<Path> candidates = new TreeSet<>(); // TreeSet makes sure iteration order is deterministic
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(translogDir)) {
            for (Path item : stream) {
                if (Files.isRegularFile(item) && Files.size(item) > 0) {
                    final String filename = item.getFileName().toString();
                    final Matcher matcher = TRANSLOG_FILE_PATTERN.matcher(filename);
                    if (filename.equals("translog.ckp") || (matcher.matches() && Long.parseLong(matcher.group(1)) >= minGeneration)) {
                        candidates.add(item);
                    }
                }
            }
        }
        assertThat("no corruption candidates found in " + translogDir, candidates, is(not(empty())));

        final Path fileToCorrupt = RandomPicks.randomFrom(random, candidates);

        // deleting the unnecessary checkpoint file doesn't count as a corruption
        final boolean maybeDelete = fileToCorrupt.equals(unnecessaryCheckpointCopyPath) == false;

        corruptFile(logger, random, fileToCorrupt, maybeDelete);
    }

    /**
     * Corrupt an (existing and nonempty) file by replacing any byte in the file with a random (different) byte, or by truncating the file
     * to a random (strictly shorter) length, or by deleting the file.
     */
    static void corruptFile(Logger logger, Random random, Path fileToCorrupt, boolean maybeDelete) throws IOException {
        assertThat(fileToCorrupt + " should be a regular file", Files.isRegularFile(fileToCorrupt));
        final long fileSize = Files.size(fileToCorrupt);
        assertThat(fileToCorrupt + " should not be an empty file", fileSize, greaterThan(0L));

        if (maybeDelete && random.nextBoolean() && random.nextBoolean()) {
            logger.info("corruptFile: deleting file {}", fileToCorrupt);
            IOUtils.rm(fileToCorrupt);
            return;
        }

        try (FileChannel fileChannel = FileChannel.open(fileToCorrupt, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            final long corruptPosition = RandomNumbers.randomLongBetween(random, 0, fileSize - 1);

            if (random.nextBoolean()) {
                // read
                fileChannel.position(corruptPosition);
                assertThat(fileChannel.position(), equalTo(corruptPosition));
                ByteBuffer bb = ByteBuffer.wrap(new byte[1]);
                fileChannel.read(bb);
                bb.flip();

                // corrupt
                byte oldValue = bb.get(0);
                byte newValue;
                do {
                    newValue = (byte) random.nextInt(0x100);
                } while (newValue == oldValue);
                bb.put(0, newValue);

                // rewrite
                fileChannel.position(corruptPosition);
                fileChannel.write(bb);
                logger.info("corruptFile: corrupting file {} at position {} turning 0x{} into 0x{}", fileToCorrupt, corruptPosition,
                    Integer.toHexString(oldValue & 0xff), Integer.toHexString(newValue & 0xff));
            } else {
                logger.info("corruptFile: truncating file {} from length {} to length {}", fileToCorrupt, fileSize, corruptPosition);
                fileChannel.truncate(corruptPosition);
            }
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

    public static Translog.Snapshot newSnapshotFromOperations(List<Translog.Operation> operations) {
        final Iterator<Translog.Operation> iterator = operations.iterator();
        return new Translog.Snapshot() {
            @Override
            public int totalOperations() {
                return operations.size();
            }

            @Override
            public Translog.Operation next() {
                if (iterator.hasNext()) {
                    return iterator.next();
                } else {
                    return null;
                }
            }

            @Override
            public void close() {

            }
        };
    }
}
