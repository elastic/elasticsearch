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
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.lease.Releasable;
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

    public void testNoRetention() throws IOException {
        long now = System.currentTimeMillis();
        Tuple<List<TranslogReader>, TranslogWriter> readersAndWriter = createReadersAndWriter(now);
        List<BaseTranslogReader> allGens = new ArrayList<>(readersAndWriter.v1());
        allGens.add(readersAndWriter.v2());
        try {
            TranslogDeletionPolicy deletionPolicy = new MockDeletionPolicy(now, 0, 0, 0);
            assertMinGenRequired(deletionPolicy, readersAndWriter, 1L);
            final int committedReader = randomIntBetween(0, allGens.size() - 1);
            final long committedGen = allGens.get(committedReader).generation;
            deletionPolicy.setTranslogGenerationOfLastCommit(randomLongBetween(committedGen, Long.MAX_VALUE));
            deletionPolicy.setMinTranslogGenerationForRecovery(committedGen);
            assertMinGenRequired(deletionPolicy, readersAndWriter, committedGen);
        } finally {
            IOUtils.close(readersAndWriter.v1());
            IOUtils.close(readersAndWriter.v2());
        }
    }

    public void testBytesRetention() throws IOException {
        long now = System.currentTimeMillis();
        Tuple<List<TranslogReader>, TranslogWriter> readersAndWriter = createReadersAndWriter(now);
        List<BaseTranslogReader> allGens = new ArrayList<>(readersAndWriter.v1());
        allGens.add(readersAndWriter.v2());
        try {
            final int selectedReader = randomIntBetween(0, allGens.size() - 1);
            final long selectedGeneration = allGens.get(selectedReader).generation;
            long size = allGens.stream().skip(selectedReader).map(BaseTranslogReader::sizeInBytes).reduce(Long::sum).get();
            assertThat(TranslogDeletionPolicy.getMinTranslogGenBySize(readersAndWriter.v1(), readersAndWriter.v2(), size),
                equalTo(selectedGeneration));
            assertThat(TranslogDeletionPolicy.getMinTranslogGenBySize(readersAndWriter.v1(), readersAndWriter.v2(), -1),
                equalTo(Long.MIN_VALUE));
        } finally {
            IOUtils.close(readersAndWriter.v1());
            IOUtils.close(readersAndWriter.v2());
        }
    }

    public void testAgeRetention() throws IOException {
        long now = System.currentTimeMillis();
        Tuple<List<TranslogReader>, TranslogWriter> readersAndWriter = createReadersAndWriter(now);
        List<BaseTranslogReader> allGens = new ArrayList<>(readersAndWriter.v1());
        allGens.add(readersAndWriter.v2());
        try {
            final int selectedReader = randomIntBetween(0, allGens.size() - 1);
            final long selectedGeneration = allGens.get(selectedReader).generation;
            long maxAge = now - allGens.get(selectedReader).getLastModifiedTime();
            assertThat(TranslogDeletionPolicy.getMinTranslogGenByAge(readersAndWriter.v1(), readersAndWriter.v2(), maxAge, now),
                equalTo(selectedGeneration));
            assertThat(TranslogDeletionPolicy.getMinTranslogGenByAge(readersAndWriter.v1(), readersAndWriter.v2(), -1, now),
                equalTo(Long.MIN_VALUE));
        } finally {
            IOUtils.close(readersAndWriter.v1());
            IOUtils.close(readersAndWriter.v2());
        }
    }

    public void testTotalFilesRetention() throws Exception {
        Tuple<List<TranslogReader>, TranslogWriter> readersAndWriter = createReadersAndWriter(System.currentTimeMillis());
        List<BaseTranslogReader> allGens = new ArrayList<>(readersAndWriter.v1());
        allGens.add(readersAndWriter.v2());
        try {
            assertThat(TranslogDeletionPolicy.getMinTranslogGenByTotalFiles(readersAndWriter.v1(), readersAndWriter.v2(),
                randomIntBetween(Integer.MIN_VALUE, 1)), equalTo(readersAndWriter.v2().generation));
            assertThat(TranslogDeletionPolicy.getMinTranslogGenByTotalFiles(readersAndWriter.v1(), readersAndWriter.v2(),
                randomIntBetween(allGens.size(), Integer.MAX_VALUE)), equalTo(allGens.get(0).generation));
            int numFiles = randomIntBetween(1, allGens.size());
            long selectedGeneration = allGens.get(allGens.size() - numFiles).generation;
            assertThat(TranslogDeletionPolicy.getMinTranslogGenByTotalFiles(readersAndWriter.v1(), readersAndWriter.v2(), numFiles),
                equalTo(selectedGeneration));
        } finally {
            IOUtils.close(readersAndWriter.v1());
            IOUtils.close(readersAndWriter.v2());
        }
    }

    /**
     * Tests that age trumps size but recovery trumps both.
     */
    public void testRetentionHierarchy() throws IOException {
        long now = System.currentTimeMillis();
        Tuple<List<TranslogReader>, TranslogWriter> readersAndWriter = createReadersAndWriter(now);
        List<BaseTranslogReader> allGens = new ArrayList<>(readersAndWriter.v1());
        allGens.add(readersAndWriter.v2());
        try {
            TranslogDeletionPolicy deletionPolicy = new MockDeletionPolicy(now, Long.MAX_VALUE, Long.MAX_VALUE, Integer.MAX_VALUE);
            deletionPolicy.setTranslogGenerationOfLastCommit(Long.MAX_VALUE);
            deletionPolicy.setMinTranslogGenerationForRecovery(Long.MAX_VALUE);
            int selectedReader = randomIntBetween(0, allGens.size() - 1);
            final long selectedGenerationByAge = allGens.get(selectedReader).generation;
            long maxAge = now - allGens.get(selectedReader).getLastModifiedTime();
            selectedReader = randomIntBetween(0, allGens.size() - 1);
            final long selectedGenerationBySize = allGens.get(selectedReader).generation;
            long size = allGens.stream().skip(selectedReader).map(BaseTranslogReader::sizeInBytes).reduce(Long::sum).get();
            selectedReader = randomIntBetween(0, allGens.size() - 1);
            final long selectedGenerationByTotalFiles = allGens.get(selectedReader).generation;
            deletionPolicy.setRetentionAgeInMillis(maxAge);
            deletionPolicy.setRetentionSizeInBytes(size);
            final int totalFiles = allGens.size() - selectedReader;
            deletionPolicy.setRetentionTotalFiles(totalFiles);
            assertMinGenRequired(deletionPolicy, readersAndWriter,
                max3(selectedGenerationByAge, selectedGenerationBySize, selectedGenerationByTotalFiles));
            // make a new policy as committed gen can't go backwards (for now)
            deletionPolicy = new MockDeletionPolicy(now, size, maxAge, totalFiles);
            long committedGen = randomFrom(allGens).generation;
            deletionPolicy.setTranslogGenerationOfLastCommit(randomLongBetween(committedGen, Long.MAX_VALUE));
            deletionPolicy.setMinTranslogGenerationForRecovery(committedGen);
            assertMinGenRequired(deletionPolicy, readersAndWriter, Math.min(committedGen,
                max3(selectedGenerationByAge, selectedGenerationBySize, selectedGenerationByTotalFiles)));
            long viewGen = randomFrom(allGens).generation;
            try (Releasable ignored = deletionPolicy.acquireTranslogGen(viewGen)) {
                assertMinGenRequired(deletionPolicy, readersAndWriter,
                    min3(committedGen, viewGen, max3(selectedGenerationByAge, selectedGenerationBySize, selectedGenerationByTotalFiles)));
                // disable age
                deletionPolicy.setRetentionAgeInMillis(-1);
                assertMinGenRequired(deletionPolicy, readersAndWriter,
                    min3(committedGen, viewGen, Math.max(selectedGenerationBySize, selectedGenerationByTotalFiles)));
                // disable size
                deletionPolicy.setRetentionAgeInMillis(maxAge);
                deletionPolicy.setRetentionSizeInBytes(-1);
                assertMinGenRequired(deletionPolicy, readersAndWriter,
                    min3(committedGen, viewGen, Math.max(selectedGenerationByAge, selectedGenerationByTotalFiles)));
                // disable age and zie
                deletionPolicy.setRetentionAgeInMillis(-1);
                deletionPolicy.setRetentionSizeInBytes(-1);
                assertMinGenRequired(deletionPolicy, readersAndWriter, Math.min(committedGen, viewGen));
                // disable total files
                deletionPolicy.setRetentionTotalFiles(0);
                assertMinGenRequired(deletionPolicy, readersAndWriter, Math.min(committedGen, viewGen));
            }
        } finally {
            IOUtils.close(readersAndWriter.v1());
            IOUtils.close(readersAndWriter.v2());
        }

    }

    private void assertMinGenRequired(TranslogDeletionPolicy deletionPolicy, Tuple<List<TranslogReader>, TranslogWriter> readersAndWriter,
                                      long expectedGen) throws IOException {
        assertThat(deletionPolicy.minTranslogGenRequired(readersAndWriter.v1(), readersAndWriter.v2()), equalTo(expectedGen));
    }

    private Tuple<List<TranslogReader>, TranslogWriter> createReadersAndWriter(final long now) throws IOException {
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
            Mockito.doReturn(now - (numberOfReaders - gen + 1) * 1000).when(writer).getLastModifiedTime();

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

    private static class MockDeletionPolicy extends TranslogDeletionPolicy {

        long now;

        MockDeletionPolicy(long now, long retentionSizeInBytes, long maxRetentionAgeInMillis, int maxRetentionTotalFiles) {
            super(retentionSizeInBytes, maxRetentionAgeInMillis, maxRetentionTotalFiles);
            this.now = now;
        }

        @Override
        protected long currentTime() {
            return now;
        }
    }

    private static long max3(long x1, long x2, long x3) {
        return Math.max(Math.max(x1, x2), x3);
    }

    private static long min3(long x1, long x2, long x3) {
        return Math.min(Math.min(x1, x2), x3);
    }
}
