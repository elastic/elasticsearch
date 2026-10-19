/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.substrate.ChunkBounds;
import org.elasticsearch.columnar.substrate.ChunkCodec;

import java.io.IOException;
import java.util.Locale;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class PlainStringColumnLayoutTests extends ColumnarStringTestCase {

    private static final int BLOCK = 128;

    private static StringColumnOptions options(ChunkCodec codec, int chunkBytes, int lengthBlockSize, boolean bounded) {
        final StringColumnOptions.Sizes defaults = StringColumnOptions.DEFAULT_SIZES;
        return new StringColumnOptions(
            DictionaryPolicy.NONE,
            SummaryPolicy.NONE,
            codec,
            new StringColumnOptions.Sizes(
                BLOCK,
                new ChunkBounds(chunkBytes, StringColumnOptions.DEFAULT_MAX_VALUES_PER_CHUNK),
                defaults.escapeChunks(),
                defaults.packedOrdinalBlockSize(),
                defaults.compressedOrdinalBlockSize(),
                defaults.escapeRankBlockSize(),
                defaults.slotCountsBlockSize(),
                lengthBlockSize
            ),
            StringColumnOptions.DEFAULT_SINGLE_VALUED,
            StringColumnOptions.DEFAULT_ENABLE_SKIP_INDEX,
            bounded
        );
    }

    private static BytesRef value(int run, int length) {
        return new BytesRef(String.format(Locale.ROOT, "%0" + length + "d", run));
    }

    private static BytesRef[][] descendingWithRepeats(int docs, int length) {
        final BytesRef[][] docSlots = new BytesRef[docs][];
        int next = 2 * docs;
        for (int doc = 0; doc < docs; doc++) {
            final int phase = doc % 250;
            if (phase == 0 || phase >= 100) {
                next--;
            }
            docSlots[doc] = new BytesRef[] { doc % 97 == 53 ? null : value(next, length) };
        }
        return docSlots;
    }

    private static BytesRef[][] descendingDistinct(int docs, int length) {
        final BytesRef[][] docSlots = new BytesRef[docs][];
        for (int doc = 0; doc < docs; doc++) {
            docSlots[doc] = new BytesRef[] { value(docs - doc, length) };
        }
        return docSlots;
    }

    private BytesRef sample(BytesRef[][] docSlots) {
        while (true) {
            final BytesRef[] slots = docSlots[between(0, docSlots.length - 1)];
            if (slots[0] != null) {
                return slots[0];
            }
        }
    }

    public void testResultsAreExactForEveryCodecAndChunkGeometry() throws IOException {
        for (ChunkCodec codec : new ChunkCodec[] { ChunkCodec.IDENTITY, ChunkCodec.ZSTD }) {
            for (int lengthBlockSize : new int[] { 128, 1024 }) {
                for (int[] shape : new int[][] { { 700, 24, 1500 }, { 4096, 24, 1500 }, { 700, 1500, 400 } }) {
                    final int chunkBytes = shape[0];
                    final int length = shape[1];
                    final BytesRef[][] docSlots = descendingWithRepeats(shape[2], length);
                    withColumn(docSlots, options(codec, chunkBytes, lengthBlockSize, true), (metadata, reader) -> {
                        final StringColumnMetadata.Plain column = (StringColumnMetadata.Plain) metadata;
                        assertNotNull(column.skipper());
                        assertThat(column.values().chunks().numChunks(), greaterThan(1));
                        for (int i = 0; i < 15; i++) {
                            final BytesRef term = sample(docSlots);
                            final BytesRef absent = new BytesRef(term.utf8ToString() + "!");
                            StringMatchOracle.assertMatches(docSlots, term::equals, () -> reader.matchTerm(term));
                            StringMatchOracle.assertMatches(docSlots, absent::equals, () -> reader.matchTerm(absent));
                            final BytesRef other = sample(docSlots);
                            final BytesRef lower = term.compareTo(other) <= 0 ? term : other;
                            final BytesRef upper = term.compareTo(other) <= 0 ? other : term;
                            StringMatchOracle.assertRange(reader, docSlots, lower, randomBoolean(), upper, randomBoolean());
                            final BytesRef prefix = new BytesRef(term.utf8ToString().substring(0, length - between(1, 3)));
                            StringMatchOracle.assertMatches(
                                docSlots,
                                candidate -> StringMatchOracle.startsWith(candidate, prefix),
                                () -> reader.matchPrefix(prefix)
                            );
                        }
                    });
                }
            }
        }
    }

    public void testChunksOfExcludedEntriesAreNotReadAndAChunkSharedWithAKeptEntryIs() throws IOException {
        final BytesRef[][] docSlots = descendingDistinct(60 * BLOCK, 24);
        final BytesRef term = docSlots[docSlots.length / 2][0];
        for (int chunkBytes : new int[] { 4096, 32768 }) {
            final long[] bytesRead = new long[2];
            final int[] chunks = new int[1];
            for (int arm = 0; arm < 2; arm++) {
                final Counter read = new Counter();
                final int index = arm;
                withColumnAndSkipIndex(
                    docSlots,
                    options(ChunkCodec.ZSTD, chunkBytes, BLOCK, arm == 0),
                    directory -> new DataBytesCountingDirectory(directory, read),
                    (metadata, reader, skip) -> {
                        final StringColumnMetadata.Plain column = (StringColumnMetadata.Plain) metadata;
                        assertEquals(index == 0, column.skipper() != null);
                        chunks[0] = column.values().chunks().numChunks();
                        final long before = read.get();
                        assertEquals(StringMatchOracle.expected(docSlots, term::equals), StringMatchOracle.matched(reader.matchTerm(term)));
                        bytesRead[index] = read.get() - before;
                    }
                );
            }
            assertThat("chunks", chunks[0], greaterThan(1));
            final long chunk = bytesRead[1] / chunks[0];
            assertThat("without the bounds every chunk is read", bytesRead[1], greaterThanOrEqualTo(bytesRead[0] * (chunks[0] / 3)));
            assertThat("with the bounds only the chunks of the kept block", bytesRead[0], lessThanOrEqualTo(3 * chunk));
            assertThat("a chunk shared with the kept block is read whole", bytesRead[0] * 2, greaterThanOrEqualTo(chunk));
        }
    }

    private static final class DataBytesCountingDirectory extends FilterDirectory {
        private final Counter bytesRead;

        DataBytesCountingDirectory(Directory in, Counter bytesRead) {
            super(in);
            this.bytesRead = bytesRead;
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            final IndexInput input = super.openInput(name, context);
            return name.endsWith(".cnd") ? new CountingInput(input, bytesRead) : input;
        }
    }

    private static final class CountingInput extends FilterIndexInput {
        private final Counter bytesRead;

        CountingInput(IndexInput in, Counter bytesRead) {
            super(in.toString(), in);
            this.bytesRead = bytesRead;
        }

        @Override
        public byte readByte() throws IOException {
            bytesRead.increment();
            return super.readByte();
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            bytesRead.add(len);
            super.readBytes(b, offset, len);
        }

        @Override
        public IndexInput clone() {
            return new CountingInput(in.clone(), bytesRead);
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
            return new CountingInput(in.slice(sliceDescription, offset, length), bytesRead);
        }
    }
}
