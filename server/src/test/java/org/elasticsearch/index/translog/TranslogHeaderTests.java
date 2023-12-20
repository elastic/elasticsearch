/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.translog;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.test.ESTestCase;

import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class TranslogHeaderTests extends ESTestCase {

    public void testCurrentHeaderVersion() throws Exception {
        final String translogUUID = UUIDs.randomBase64UUID();
        final TranslogHeader outHeader = new TranslogHeader(translogUUID, randomNonNegativeLong());
        final long generation = randomNonNegativeLong();
        final Path translogFile = createTempDir().resolve(Translog.getFilename(generation));
        try (FileChannel channel = FileChannel.open(translogFile, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            outHeader.write(channel);
            assertThat(outHeader.sizeInBytes(), equalTo((int) channel.position()));
        }
        try (FileChannel channel = FileChannel.open(translogFile, StandardOpenOption.READ)) {
            final TranslogHeader inHeader = TranslogHeader.read(translogUUID, translogFile, channel);
            assertThat(inHeader.getTranslogUUID(), equalTo(translogUUID));
            assertThat(inHeader.getPrimaryTerm(), equalTo(outHeader.getPrimaryTerm()));
            assertThat(inHeader.sizeInBytes(), equalTo((int) channel.position()));
        }
        final TranslogCorruptedException mismatchUUID = expectThrows(TranslogCorruptedException.class, () -> {
            try (FileChannel channel = FileChannel.open(translogFile, StandardOpenOption.READ)) {
                TranslogHeader.read(randomValueOtherThan(translogUUID, UUIDs::randomBase64UUID), translogFile, channel);
            }
        });
        assertThat(mismatchUUID.getMessage(), containsString("this translog file belongs to a different translog"));
        TestTranslog.corruptFile(logger, random(), translogFile, false);
        final TranslogCorruptedException corruption = expectThrows(TranslogCorruptedException.class, () -> {
            try (FileChannel channel = FileChannel.open(translogFile, StandardOpenOption.READ)) {
                TranslogHeader.read(randomBoolean() ? outHeader.getTranslogUUID() : UUIDs.randomBase64UUID(), translogFile, channel);
            }
        });
        assertThat(corruption.getMessage(), not(containsString("this translog file belongs to a different translog")));
    }

    public void testLegacyTranslogVersions() {
        final String expectedMessage = "translog header corrupted";
        checkFailsToOpen("/org/elasticsearch/index/translog/translog-v0.binary", TranslogCorruptedException.class, expectedMessage);
        checkFailsToOpen("/org/elasticsearch/index/translog/translog-v1.binary", TranslogCorruptedException.class, expectedMessage);
        checkFailsToOpen("/org/elasticsearch/index/translog/translog-v2.binary", TranslogCorruptedException.class, expectedMessage);
        checkFailsToOpen(
            "/org/elasticsearch/index/translog/translog-v1-truncated.binary",
            TranslogCorruptedException.class,
            expectedMessage
        );
        checkFailsToOpen(
            "/org/elasticsearch/index/translog/translog-v1-corrupted-magic.binary",
            TranslogCorruptedException.class,
            expectedMessage
        );
        checkFailsToOpen(
            "/org/elasticsearch/index/translog/translog-v1-corrupted-body.binary",
            TranslogCorruptedException.class,
            expectedMessage
        );
    }

    public void testCorruptTranslogHeader() throws Exception {
        final String translogUUID = UUIDs.randomBase64UUID();
        final TranslogHeader outHeader = new TranslogHeader(translogUUID, randomNonNegativeLong());
        final long generation = randomNonNegativeLong();
        final Path translogLocation = createTempDir();
        final Path translogFile = translogLocation.resolve(Translog.getFilename(generation));
        try (FileChannel channel = FileChannel.open(translogFile, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            outHeader.write(channel);
            assertThat(outHeader.sizeInBytes(), equalTo((int) channel.position()));
        }
        TestTranslog.corruptFile(logger, random(), translogFile, false);
        final Exception error = expectThrows(Exception.class, () -> {
            try (FileChannel channel = FileChannel.open(translogFile, StandardOpenOption.READ)) {
                TranslogHeader.read(randomValueOtherThan(translogUUID, UUIDs::randomBase64UUID), translogFile, channel);
            }
        });
        assertThat(error, instanceOf(TranslogCorruptedException.class));
    }

    private <E extends Exception> void checkFailsToOpen(String file, Class<E> expectedErrorType, String expectedMessage) {
        final Path translogFile = getDataPath(file);
        assertThat("test file [" + translogFile + "] should exist", Files.exists(translogFile), equalTo(true));
        final E error = expectThrows(expectedErrorType, () -> {
            final Checkpoint checkpoint = new Checkpoint(
                Files.size(translogFile),
                1,
                1,
                SequenceNumbers.NO_OPS_PERFORMED,
                SequenceNumbers.NO_OPS_PERFORMED,
                SequenceNumbers.NO_OPS_PERFORMED,
                1,
                SequenceNumbers.NO_OPS_PERFORMED
            );
            try (FileChannel channel = FileChannel.open(translogFile, StandardOpenOption.READ)) {
                TranslogReader.open(channel, translogFile, checkpoint, null);
            }
        });
        assertThat(error.getMessage(), containsString(expectedMessage));
    }
}
