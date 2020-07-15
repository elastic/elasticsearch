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
package org.elasticsearch.test;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Random;

import static org.apache.lucene.util.LuceneTestCase.assumeTrue;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


public final class CorruptionUtils {
    private static final Logger logger = LogManager.getLogger(CorruptionUtils.class);
    private CorruptionUtils() {}

    public static void corruptIndex(Random random, Path indexPath, boolean corruptSegments) throws IOException {
        // corrupt files
        final Path[] filesToCorrupt =
            Files.walk(indexPath)
                .filter(p -> {
                    final String name = p.getFileName().toString();
                    boolean segmentFile = name.startsWith("segments_") || name.endsWith(".si");
                        return Files.isRegularFile(p)
                            && name.startsWith("extra") == false // Skip files added by Lucene's ExtrasFS
                            && IndexWriter.WRITE_LOCK_NAME.equals(name) == false
                            && (corruptSegments ? segmentFile : segmentFile == false);
                    }
                )
                .toArray(Path[]::new);
        corruptFile(random, filesToCorrupt);
    }

    /**
     * Corrupts a random file at a random position
     */
    public static void corruptFile(Random random, Path... files) throws IOException {
        assertTrue("files must be non-empty", files.length > 0);
        final Path fileToCorrupt = RandomPicks.randomFrom(random, files);
        assertTrue(fileToCorrupt + " is not a file", Files.isRegularFile(fileToCorrupt));
        try (Directory dir = FSDirectory.open(fileToCorrupt.toAbsolutePath().getParent())) {
            long checksumBeforeCorruption;
            try (IndexInput input = dir.openInput(fileToCorrupt.getFileName().toString(), IOContext.DEFAULT)) {
                checksumBeforeCorruption = CodecUtil.retrieveChecksum(input);
            }
            try (FileChannel raf = FileChannel.open(fileToCorrupt, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                long maxPosition = raf.size();
                final int position = random.nextInt((int) Math.min(Integer.MAX_VALUE, maxPosition));
                corruptAt(fileToCorrupt, raf, position);
            }

            long checksumAfterCorruption;
            long actualChecksumAfterCorruption;

            try (ChecksumIndexInput input = dir.openChecksumInput(fileToCorrupt.getFileName().toString(), IOContext.DEFAULT)) {
                assertThat(input.getFilePointer(), is(0L));
                input.seek(input.length() - CodecUtil.footerLength());
                checksumAfterCorruption = input.getChecksum();
                input.seek(input.length() - 8);
                actualChecksumAfterCorruption = input.readLong();
            }
            // we need to add assumptions here that the checksums actually really don't match there is a small chance to get collisions
            // in the checksum which is ok though....
            StringBuilder msg = new StringBuilder();
            msg.append("before: [").append(checksumBeforeCorruption).append("] ");
            msg.append("after: [").append(checksumAfterCorruption).append("] ");
            msg.append("checksum value after corruption: ").append(actualChecksumAfterCorruption).append("] ");
            msg.append("file: ").append(fileToCorrupt.getFileName()).append(" length: ");
            msg.append(dir.fileLength(fileToCorrupt.getFileName().toString()));
            logger.info("Checksum {}", msg);
            assumeTrue("Checksum collision - " + msg.toString(),
                    checksumAfterCorruption != checksumBeforeCorruption // collision
                            || actualChecksumAfterCorruption != checksumBeforeCorruption); // checksum corrupted
            assertThat("no file corrupted", fileToCorrupt, notNullValue());
        }
    }

    static void corruptAt(Path path, FileChannel channel, int position) throws IOException {
        // read
        channel.position(position);
        long filePointer = channel.position();
        ByteBuffer bb = ByteBuffer.wrap(new byte[1]);
        channel.read(bb);
        bb.flip();

        // corrupt
        byte oldValue = bb.get(0);
        byte newValue = (byte) (oldValue + 1);
        bb.put(0, newValue);

        // rewrite
        channel.position(filePointer);
        channel.write(bb);
        logger.info("Corrupting file --  flipping at position {} from {} to {} file: {}", filePointer,
                Integer.toHexString(oldValue), Integer.toHexString(newValue), path.getFileName());
    }


}
