/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.translog;

import org.apache.lucene.store.ByteArrayDataOutput;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Tuple;
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
            assertThat(deletionPolicy.getMinTranslogGenRequiredByLocks(), equalTo(Long.MAX_VALUE));

            long gen1 = randomIntBetween(0, allGens.size() - 1);
            Releasable releaseGen1 = deletionPolicy.acquireTranslogGen(gen1);
            assertThat(deletionPolicy.getMinTranslogGenRequiredByLocks(), equalTo(gen1));

            long gen2 = randomIntBetween(0, allGens.size() - 1);
            Releasable releaseGen2 = deletionPolicy.acquireTranslogGen(gen2);
            assertThat(deletionPolicy.getMinTranslogGenRequiredByLocks(), equalTo(Math.min(gen1, gen2)));

            if (randomBoolean()) {
                releaseGen1.close();
                assertThat(deletionPolicy.getMinTranslogGenRequiredByLocks(), equalTo(gen2));
                releaseGen2.close();
            } else {
                releaseGen2.close();
                assertThat(deletionPolicy.getMinTranslogGenRequiredByLocks(), equalTo(gen1));
                releaseGen1.close();
            }
            assertThat(deletionPolicy.getMinTranslogGenRequiredByLocks(), equalTo(Long.MAX_VALUE));

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
            writer = TranslogWriter.create(
                new ShardId("index", "uuid", 0),
                translogUUID,
                gen,
                tempDir.resolve(Translog.getFilename(gen)),
                FileChannel::open,
                TranslogConfig.DEFAULT_BUFFER_SIZE,
                1L,
                1L,
                () -> 1L,
                () -> 1L,
                randomNonNegativeLong(),
                new TragicExceptionHolder(),
                seqNo -> {},
                BigArrays.NON_RECYCLING_INSTANCE,
                TranslogTests.RANDOMIZING_IO_BUFFERS,
                (d, s, l) -> {}
            );
            writer = Mockito.spy(writer);
            byte[] bytes = new byte[4];
            ByteArrayDataOutput out = new ByteArrayDataOutput(bytes);

            for (int ops = randomIntBetween(0, 20); ops > 0; ops--) {
                out.reset(bytes);
                out.writeInt(ops);
                writer.add(ReleasableBytesReference.wrap(new BytesArray(bytes)), ops);
            }
        }
        return new Tuple<>(readers, writer);
    }
}
