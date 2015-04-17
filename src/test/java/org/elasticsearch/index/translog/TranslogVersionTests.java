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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.EOFException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for reading old and new translog files
 */
public class TranslogVersionTests extends ElasticsearchTestCase {

    @Test
    public void testV0LegacyTranslogVersion() throws Exception {
        Path translogFile = getDataPath("/org/elasticsearch/index/translog/translog-v0.binary");
        assertThat("test file should exist", Files.exists(translogFile), equalTo(true));
        TranslogStream stream = TranslogStreams.translogStreamFor(translogFile);
        assertThat("a version0 stream is returned", stream instanceof LegacyTranslogStream, equalTo(true));

        StreamInput in = stream.openInput(translogFile);

        in.readInt();
        Translog.Operation operation = stream.read(in);

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

        try {
            in.readInt();
            stream.read(in);
            fail("should have been the end of the file");
        } catch (EOFException e) {
            // success
        }
        in.close();
    }

    @Test
    public void testV1ChecksummedTranslogVersion() throws Exception {
        Path translogFile = getDataPath("/org/elasticsearch/index/translog/translog-v1.binary");
        assertThat("test file should exist", Files.exists(translogFile), equalTo(true));
        TranslogStream stream = TranslogStreams.translogStreamFor(translogFile);
        assertThat("a version1 stream is returned", stream instanceof ChecksummedTranslogStream, equalTo(true));

        StreamInput in = stream.openInput(translogFile);
        Translog.Operation operation = stream.read(in);

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
        while (true) {
            try {
                stream.read(in);
                opNum++;
            } catch (EOFException e) {
                break;
            }
        }
        assertThat("there should be 5 translog operations", opNum, equalTo(5));
        in.close();
    }

    @Test
    public void testCorruptedTranslogs() throws Exception {
        try {
            Path translogFile = getDataPath("/org/elasticsearch/index/translog/translog-v1-corrupted-magic.binary");
            assertThat("test file should exist", Files.exists(translogFile), equalTo(true));
            TranslogStream stream = TranslogStreams.translogStreamFor(translogFile);
            fail("should have thrown an exception about the header being corrupt");
        } catch (TranslogCorruptedException e) {
            assertThat("translog corruption from header: " + e.getMessage(),
                    e.getMessage().contains("translog looks like version 1 or later, but has corrupted header"), equalTo(true));
        }

        try {
            Path translogFile = getDataPath("/org/elasticsearch/index/translog/translog-invalid-first-byte.binary");
            assertThat("test file should exist", Files.exists(translogFile), equalTo(true));
            TranslogStream stream = TranslogStreams.translogStreamFor(translogFile);
            fail("should have thrown an exception about the header being corrupt");
        } catch (TranslogCorruptedException e) {
            assertThat("translog corruption from header: " + e.getMessage(),
                    e.getMessage().contains("Invalid first byte in translog file, got: 1, expected 0x00 or 0x3f"), equalTo(true));
        }

        try {
            Path translogFile = getDataPath("/org/elasticsearch/index/translog/translog-v1-corrupted-body.binary");
            assertThat("test file should exist", Files.exists(translogFile), equalTo(true));
            TranslogStream stream = TranslogStreams.translogStreamFor(translogFile);
            try (StreamInput in = stream.openInput(translogFile)) {
                while (true) {
                    try {
                        stream.read(in);
                    } catch (EOFException e) {
                        break;
                    }
                }
            }
            fail("should have thrown an exception about the body being corrupted");
        } catch (TranslogCorruptedException e) {
            assertThat("translog corruption from body: " + e.getMessage(),
                    e.getMessage().contains("translog stream is corrupted"), equalTo(true));
        }

    }

    @Test
    public void testTruncatedTranslog() throws Exception {
        try {
            Path translogFile = getDataPath("/org/elasticsearch/index/translog/translog-v1-truncated.binary");
            assertThat("test file should exist", Files.exists(translogFile), equalTo(true));
            TranslogStream stream = TranslogStreams.translogStreamFor(translogFile);
            try (StreamInput in = stream.openInput(translogFile)) {
                while (true) {
                    try {
                        stream.read(in);
                    } catch (EOFException e) {
                        break;
                    }
                }
            }
            fail("should have thrown an exception about the body being truncated");
        } catch (TruncatedTranslogException e) {
            assertThat("translog truncated: " + e.getMessage(),
                    e.getMessage().contains("reached premature end of file, translog is truncated"), equalTo(true));
        }
    }
}
