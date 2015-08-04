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

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for reading old and new translog files
 */
public class TranslogVersionTests extends ESTestCase {

    @Test
    public void testV0LegacyTranslogVersion() throws Exception {
        Path translogFile = getDataPath("/org/elasticsearch/index/translog/translog-v0.binary");
        assertThat("test file should exist", Files.exists(translogFile), equalTo(true));
        try (ImmutableTranslogReader reader = openReader(translogFile, 0)) {
            assertThat("a version0 stream is returned", reader instanceof LegacyTranslogReader, equalTo(true));
            try (final Translog.Snapshot snapshot = reader.newSnapshot()) {
                final Translog.Operation operation = snapshot.next();
                assertThat("operation is the correct type correctly", operation.opType() == Translog.Operation.Type.SAVE, equalTo(true));
                Translog.Index op = (Translog.Index) operation;
                assertThat(op.id(), equalTo("1"));
                assertThat(op.type(), equalTo("doc"));
                assertThat(op.source().toUtf8(), equalTo("{\"body\": \"worda wordb wordc wordd \\\"worde\\\" wordf\"}"));
                assertThat(op.routing(), equalTo(null));
                assertThat(op.parent(), equalTo(null));
                assertThat(op.version(), equalTo(1L));
                assertThat(op.timestamp(), equalTo(1407312091791L));
                assertThat(op.ttl(), equalTo(-1L));
                assertThat(op.versionType(), equalTo(VersionType.INTERNAL));

                assertNull(snapshot.next());
            }
        }
    }

    @Test
    public void testV1ChecksummedTranslogVersion() throws Exception {
        Path translogFile = getDataPath("/org/elasticsearch/index/translog/translog-v1.binary");
        assertThat("test file should exist", Files.exists(translogFile), equalTo(true));
        try (ImmutableTranslogReader reader = openReader(translogFile, 0)) {
            try (final Translog.Snapshot snapshot = reader.newSnapshot()) {

                assertThat("a version1 stream is returned", reader instanceof ImmutableTranslogReader, equalTo(true));

                Translog.Operation operation = snapshot.next();

                assertThat("operation is the correct type correctly", operation.opType() == Translog.Operation.Type.CREATE, equalTo(true));
                Translog.Create op = (Translog.Create) operation;
                assertThat(op.id(), equalTo("Bwiq98KFSb6YjJQGeSpeiw"));
                assertThat(op.type(), equalTo("doc"));
                assertThat(op.source().toUtf8(), equalTo("{\"body\": \"foo\"}"));
                assertThat(op.routing(), equalTo(null));
                assertThat(op.parent(), equalTo(null));
                assertThat(op.version(), equalTo(1L));
                assertThat(op.timestamp(), equalTo(1408627184844L));
                assertThat(op.ttl(), equalTo(-1L));
                assertThat(op.versionType(), equalTo(VersionType.INTERNAL));

                // There are more operations
                int opNum = 1;
                while (snapshot.next() != null) {
                    opNum++;
                }
                assertThat("there should be 5 translog operations", opNum, equalTo(5));
            }
        }
    }

    @Test
    public void testCorruptedTranslogs() throws Exception {
        try {
            Path translogFile = getDataPath("/org/elasticsearch/index/translog/translog-v1-corrupted-magic.binary");
            assertThat("test file should exist", Files.exists(translogFile), equalTo(true));
            openReader(translogFile, 0);
            fail("should have thrown an exception about the header being corrupt");
        } catch (TranslogCorruptedException e) {
            assertThat("translog corruption from header: " + e.getMessage(),
                    e.getMessage().contains("translog looks like version 1 or later, but has corrupted header"), equalTo(true));
        }

        try {
            Path translogFile = getDataPath("/org/elasticsearch/index/translog/translog-invalid-first-byte.binary");
            assertThat("test file should exist", Files.exists(translogFile), equalTo(true));
            openReader(translogFile, 0);
            fail("should have thrown an exception about the header being corrupt");
        } catch (TranslogCorruptedException e) {
            assertThat("translog corruption from header: " + e.getMessage(),
                    e.getMessage().contains("Invalid first byte in translog file, got: 1, expected 0x00 or 0x3f"), equalTo(true));
        }

        try {
            Path translogFile = getDataPath("/org/elasticsearch/index/translog/translog-v1-corrupted-body.binary");
            assertThat("test file should exist", Files.exists(translogFile), equalTo(true));
            try (ImmutableTranslogReader reader = openReader(translogFile, 0)) {
                try (final Translog.Snapshot snapshot = reader.newSnapshot()) {
                    while(snapshot.next() != null) {

                    }
                }
            }
            fail("should have thrown an exception about the body being corrupted");
        } catch (TranslogCorruptedException e) {
            assertThat("translog corruption from body: " + e.getMessage(),
                    e.getMessage().contains("translog corruption while reading from stream"), equalTo(true));
        }

    }

    @Test
    public void testTruncatedTranslog() throws Exception {
        try {
            Path translogFile = getDataPath("/org/elasticsearch/index/translog/translog-v1-truncated.binary");
            assertThat("test file should exist", Files.exists(translogFile), equalTo(true));
            try (ImmutableTranslogReader reader = openReader(translogFile, 0)) {
                try (final Translog.Snapshot snapshot = reader.newSnapshot()) {
                    while(snapshot.next() != null) {

                    }
                }
            }
            fail("should have thrown an exception about the body being truncated");
        } catch (TranslogCorruptedException e) {
            assertThat("translog truncated: " + e.getMessage(),
                    e.getMessage().contains("operation size is corrupted must be"), equalTo(true));
        }
    }

    public ImmutableTranslogReader openReader(Path path, long id) throws IOException {
        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
        try {
            final ChannelReference raf = new ChannelReference(path, id, channel, null);
            ImmutableTranslogReader reader = ImmutableTranslogReader.open(raf, new Checkpoint(Files.size(path), TranslogReader.UNKNOWN_OP_COUNT, id), null);
            channel = null;
            return reader;
        } finally {
            IOUtils.close(channel);
        }
    }
}
