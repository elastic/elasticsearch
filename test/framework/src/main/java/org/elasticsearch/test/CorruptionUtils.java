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
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.common.logging.ESLoggerFactory;

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
    private static Logger logger = ESLoggerFactory.getLogger("test");
    private CorruptionUtils() {}

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
                // read
                raf.position(random.nextInt((int) Math.min(Integer.MAX_VALUE, raf.size())));
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
                logger.info("Corrupting file --  flipping at position {} from {} to {} file: {}", filePointer, Integer.toHexString(oldValue), Integer.toHexString(newValue), fileToCorrupt.getFileName());
            }
            long checksumAfterCorruption;
            long actualChecksumAfterCorruption;
            try (ChecksumIndexInput input = dir.openChecksumInput(fileToCorrupt.getFileName().toString(), IOContext.DEFAULT)) {
                assertThat(input.getFilePointer(), is(0L));
                input.seek(input.length() - 8); // one long is the checksum... 8 bytes
                checksumAfterCorruption = input.getChecksum();
                actualChecksumAfterCorruption = input.readLong();
            }
            // we need to add assumptions here that the checksums actually really don't match there is a small chance to get collisions
            // in the checksum which is ok though....
            StringBuilder msg = new StringBuilder();
            msg.append("before: [").append(checksumBeforeCorruption).append("] ");
            msg.append("after: [").append(checksumAfterCorruption).append("] ");
            msg.append("checksum value after corruption: ").append(actualChecksumAfterCorruption).append("] ");
            msg.append("file: ").append(fileToCorrupt.getFileName()).append(" length: ").append(dir.fileLength(fileToCorrupt.getFileName().toString()));
            logger.info("Checksum {}", msg);
            assumeTrue("Checksum collision - " + msg.toString(),
                    checksumAfterCorruption != checksumBeforeCorruption // collision
                            || actualChecksumAfterCorruption != checksumBeforeCorruption); // checksum corrupted
            assertThat("no file corrupted", fileToCorrupt, notNullValue());
        }
    }


}
