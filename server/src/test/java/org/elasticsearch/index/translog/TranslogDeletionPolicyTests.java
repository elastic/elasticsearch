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

import org.apache.lucene.store.ByteArrayDataOutput;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class TranslogDeletionPolicyTests extends ESTestCase {

    public void testMinRetainedGeneration() throws IOException {
        Tuple<List<TranslogReader>, TranslogWriter> readersAndWriter = createReadersAndWriter();
        List<BaseTranslogReader> allGens = new ArrayList<>(readersAndWriter.v1());
        allGens.add(readersAndWriter.v2());
        try {
            TranslogDeletionPolicy deletionPolicy = new TranslogDeletionPolicy();
            assertThat(deletionPolicy.minTranslogGenRequired(), equalTo(1L));
            final int committedReader = randomIntBetween(0, allGens.size() - 1);
            final long committedGen = allGens.get(committedReader).generation;
            deletionPolicy.setTranslogGenerationOfLastCommit(randomLongBetween(committedGen, Long.MAX_VALUE));
            deletionPolicy.setMinTranslogGenerationForRecovery(committedGen);
            assertThat(deletionPolicy.minTranslogGenRequired(), equalTo(committedGen));

            long gen1 = randomIntBetween(0, allGens.size() - 1);
            Releasable releaseGen1 = deletionPolicy.acquireTranslogGen(gen1);
            assertThat(deletionPolicy.minTranslogGenRequired(),
                equalTo(Math.min(gen1, committedGen)));

            long gen2 = randomIntBetween(0, allGens.size() - 1);
            Releasable releaseGen2 = deletionPolicy.acquireTranslogGen(gen2);
            assertThat(deletionPolicy.minTranslogGenRequired(),
                equalTo(Math.min(Math.min(gen1, gen2), committedGen)));

            if (randomBoolean()) {
                releaseGen1.close();
                assertThat(deletionPolicy.minTranslogGenRequired(),
                    equalTo(Math.min(gen2, committedGen)));
                releaseGen2.close();
            } else {
                releaseGen2.close();
                assertThat(deletionPolicy.minTranslogGenRequired(),
                    equalTo(Math.min(gen1, committedGen)));
                releaseGen1.close();
            }
            assertThat(deletionPolicy.minTranslogGenRequired(), equalTo(committedGen));

        } finally {
            IOUtils.close(readersAndWriter.v1());
            IOUtils.close(readersAndWriter.v2());
        }
    }

    private Tuple<List<TranslogReader>, TranslogWriter> createReadersAndWriter() throws IOException {
        final Path tempDir = createTempDir();
        Files.createFile(tempDir.resolve(Translog.CHECKPOINT_FILE_NAME));
        TranslogWriter writer = null;
        List<TranslogReader> readers = new ArrayList<>();
        final int numberOfReaders = randomIntBetween(0, 10);
        final String translogUUID = UUIDs.randomBase64UUID(random());
        for (long gen = 1; gen <= numberOfReaders + 1; gen++) {
            if (writer != null) {
                final TranslogReader reader = Mockito.spy(writer.closeIntoReader());
                Mockito.doReturn(writer.getLastModifiedTime()).when(reader).getLastModifiedTime();
                readers.add(reader);
            }
            writer = TranslogWriter.create(new ShardId("index", "uuid", 0), translogUUID, gen,
                tempDir.resolve(Translog.getFilename(gen)), FileChannel::open, TranslogConfig.DEFAULT_BUFFER_SIZE, 1L, 1L, () -> 1L,
                () -> 1L, randomNonNegativeLong(), new TragicExceptionHolder(), seqNo -> {});
            writer = Mockito.spy(writer);
            byte[] bytes = new byte[4];
            ByteArrayDataOutput out = new ByteArrayDataOutput(bytes);

            for (int ops = randomIntBetween(0, 20); ops > 0; ops--) {
                out.reset(bytes);
                out.writeInt(ops);
                writer.add(new BytesArray(bytes), ops);
            }
        }
        return new Tuple<>(readers, writer);
    }
}
