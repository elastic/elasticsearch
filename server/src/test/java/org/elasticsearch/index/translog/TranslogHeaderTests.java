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

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

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
        for (int i = 0; i < corruptions; i++) {
            TestTranslog.corruptFile(logger, random(), translogFile);
        }
        expectThrows(TranslogCorruptedException.class, () -> {
            try (FileChannel channel = FileChannel.open(translogFile, StandardOpenOption.READ)) {
                TranslogHeader.read(outHeader.getTranslogUUID(), translogFile, channel);
            }
        });
    }

    public void testHeaderWithoutPrimaryTerm() throws Exception {
        final String translogUUID = UUIDs.randomBase64UUID();
        final long generation = randomNonNegativeLong();
        final Path translogFile = createTempDir().resolve(Translog.getFilename(generation));
        try (FileChannel channel = FileChannel.open(translogFile, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            writeLegacyTranslogHeader(channel, translogUUID);
            assertThat((int)channel.position(), lessThan(TranslogHeader.defaultSizeInBytes(translogUUID)));
        }
        try (FileChannel channel = FileChannel.open(translogFile, StandardOpenOption.READ)) {
            final TranslogHeader inHeader = TranslogHeader.read(translogUUID, translogFile, channel);
            assertThat(inHeader.getTranslogUUID(), equalTo(translogUUID));
            assertThat(inHeader.getPrimaryTerm(), equalTo(TranslogHeader.UNKNOWN_PRIMARY_TERM));
            assertThat(inHeader.sizeInBytes(), equalTo((int)channel.position()));
        }
        expectThrows(TranslogCorruptedException.class, () -> {
            try (FileChannel channel = FileChannel.open(translogFile, StandardOpenOption.READ)) {
                TranslogHeader.read(UUIDs.randomBase64UUID(), translogFile, channel);
            }
        });
    }

    static void writeLegacyTranslogHeader(FileChannel channel, String translogUUID) throws IOException {
        final OutputStreamStreamOutput out = new OutputStreamStreamOutput(Channels.newOutputStream(channel));
        CodecUtil.writeHeader(new OutputStreamDataOutput(out), TranslogHeader.TRANSLOG_CODEC, TranslogHeader.VERSION_CHECKPOINTS);
        final BytesRef uuid = new BytesRef(translogUUID);
        out.writeInt(uuid.length);
        out.writeBytes(uuid.bytes, uuid.offset, uuid.length);
        channel.force(true);
        assertThat(channel.position(), equalTo(43L));
    }
}
