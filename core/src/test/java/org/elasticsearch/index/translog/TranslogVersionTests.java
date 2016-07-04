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
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for reading old and new translog files
 */
public class TranslogVersionTests extends ESTestCase {

    private void checkFailsToOpen(String file, String expectedMessage) throws IOException {
        Path translogFile = getDataPath(file);
        assertThat("test file should exist", Files.exists(translogFile), equalTo(true));
        try {
            openReader(translogFile, 0);
            fail("should be able to open an old translog");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), containsString(expectedMessage));
        }

    }

    public void testV0LegacyTranslogVersion() throws Exception {
        checkFailsToOpen("/org/elasticsearch/index/translog/translog-v0.binary", "pre-1.4 translog");
    }

    public void testV1ChecksummedTranslogVersion() throws Exception {
        checkFailsToOpen("/org/elasticsearch/index/translog/translog-v1.binary", "pre-2.0 translog");
    }

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

        checkFailsToOpen("/org/elasticsearch/index/translog/translog-v1-corrupted-body.binary", "pre-2.0 translog");
    }

    public void testTruncatedTranslog() throws Exception {
        checkFailsToOpen("/org/elasticsearch/index/translog/translog-v1-truncated.binary", "pre-2.0 translog");
    }

    public TranslogReader openReader(Path path, long id) throws IOException {
        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
        try {
            TranslogReader reader = TranslogReader.open(channel, path, new Checkpoint(Files.size(path), 1, id), null);
            channel = null;
            return reader;
        } finally {
            IOUtils.close(channel);
        }
    }
}
