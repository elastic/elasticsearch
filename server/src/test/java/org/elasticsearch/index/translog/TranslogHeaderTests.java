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

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.test.ESTestCase;

import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class TranslogHeaderTests extends ESTestCase {

    public void testCurrentHeaderVersion() throws Exception {
        final String translogUUID = UUIDs.randomBase64UUID();
        final TranslogHeader outHeader = new TranslogHeader(translogUUID, randomNonNegativeLong());
        final long generation = randomNonNegativeLong();
        final Path translogFile = createTempDir().resolve(Translog.getFilename(generation));
        try (FileChannel channel = FileChannel.open(translogFile, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            outHeader.write(channel);
            assertThat(outHeader.sizeInBytes(), equalTo((int)channel.position()));
        }
        try (FileChannel channel = FileChannel.open(translogFile, StandardOpenOption.READ)) {
            final TranslogHeader inHeader = TranslogHeader.read(translogUUID, translogFile, channel);
            assertThat(inHeader.getTranslogUUID(), equalTo(translogUUID));
            assertThat(inHeader.getPrimaryTerm(), equalTo(outHeader.getPrimaryTerm()));
            assertThat(inHeader.sizeInBytes(), equalTo((int)channel.position()));
        }
        final TranslogCorruptedException mismatchUUID = expectThrows(TranslogCorruptedException.class, () -> {
            try (FileChannel channel = FileChannel.open(translogFile, StandardOpenOption.READ)) {
                TranslogHeader.read(UUIDs.randomBase64UUID(), translogFile, channel);
            }
        });
        assertThat(mismatchUUID.getMessage(), containsString("this translog file belongs to a different translog"));
        int corruptions = between(1, 10);
        for (int i = 0; i < corruptions && Files.size(translogFile) > 0; i++) {
            TestTranslog.corruptFile(logger, random(), translogFile, false);
        }
        expectThrows(TranslogCorruptedException.class, () -> {
            try (FileChannel channel = FileChannel.open(translogFile, StandardOpenOption.READ)) {
                TranslogHeader.read(outHeader.getTranslogUUID(), translogFile, channel);
            } catch (IllegalStateException e) {
                // corruption corrupted the version byte making this look like a v2, v1 or v0 translog
                assertThat("version " + TranslogHeader.VERSION_CHECKPOINTS + "-or-earlier translog",
                    e.getMessage(), anyOf(containsString("pre-2.0 translog found"), containsString("pre-1.4 translog found"),
                        containsString("pre-6.3 translog found")));
                throw new TranslogCorruptedException(translogFile.toString(), "adjusted translog version", e);
            }
        });
    }

    public void testLegacyTranslogVersions() {
        checkFailsToOpen("/org/elasticsearch/index/translog/translog-v0.binary", IllegalStateException.class, "pre-1.4 translog");
        checkFailsToOpen("/org/elasticsearch/index/translog/translog-v1.binary", IllegalStateException.class, "pre-2.0 translog");
        checkFailsToOpen("/org/elasticsearch/index/translog/translog-v2.binary", IllegalStateException.class, "pre-6.3 translog");
        checkFailsToOpen("/org/elasticsearch/index/translog/translog-v1-truncated.binary", IllegalStateException.class, "pre-2.0 translog");
        checkFailsToOpen("/org/elasticsearch/index/translog/translog-v1-corrupted-magic.binary",
            TranslogCorruptedException.class, "translog looks like version 1 or later, but has corrupted header");
        checkFailsToOpen("/org/elasticsearch/index/translog/translog-v1-corrupted-body.binary",
            IllegalStateException.class, "pre-2.0 translog");
    }

    private <E extends Exception> void checkFailsToOpen(String file, Class<E> expectedErrorType, String expectedMessage) {
        final Path translogFile = getDataPath(file);
        assertThat("test file [" + translogFile + "] should exist", Files.exists(translogFile), equalTo(true));
        final E error = expectThrows(expectedErrorType, () -> {
            final Checkpoint checkpoint = new Checkpoint(Files.size(translogFile), 1, 1,
                SequenceNumbers.NO_OPS_PERFORMED, SequenceNumbers.NO_OPS_PERFORMED,
                SequenceNumbers.NO_OPS_PERFORMED, 1, SequenceNumbers.NO_OPS_PERFORMED);
            try (FileChannel channel = FileChannel.open(translogFile, StandardOpenOption.READ)) {
                TranslogReader.open(channel, translogFile, checkpoint, null);
            }
        });
        assertThat(error.getMessage(), containsString(expectedMessage));
    }
}
