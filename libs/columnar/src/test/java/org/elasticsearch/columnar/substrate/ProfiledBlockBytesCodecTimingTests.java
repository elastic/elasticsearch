/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.substrate;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.codec.profiler.ProfileCollector;
import org.elasticsearch.codec.profiler.TestCollectors;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class ProfiledBlockBytesCodecTimingTests extends ESTestCase {

    private interface Read {
        DataInput read(IndexInput in, int length) throws IOException;
    }

    private static BlockBytesCodec codec(Read read) {
        return new BlockBytesCodec() {
            @Override
            public byte id() {
                return 0;
            }

            @Override
            public void write(BlockEncoder encoder, DataOutput out) {
                throw new UnsupportedOperationException();
            }

            @Override
            public DataInput read(IndexInput in, int length) throws IOException {
                return read.read(in, length);
            }
        };
    }

    private static IndexInput input(Directory dir) throws IOException {
        try (IndexOutput out = dir.createOutput("block", IOContext.DEFAULT)) {
            out.writeByte((byte) 0);
        }
        return dir.openInput("block", IOContext.DEFAULT);
    }

    public void testBlockDecompressionNestedInABlockReadIsReportedBySeparateBoundariesAndNeverTwice() throws IOException {
        try (Directory dir = new ByteBuffersDirectory(); IndexInput in = input(dir)) {
            final long[] now = { 3 };
            final ProfileCollector stats = TestCollectors.withClock(() -> now[0]);
            final ChunkDecompressor inflater = new ProfiledChunkDecompressor(
                (input, stored, dst, inflated) -> now[0] += 60,
                stats,
                ProfiledBlockBytesCodec.INFLATION
            );
            final BlockBytesCodec profiled = new ProfiledBlockBytesCodec(codec((input, length) -> {
                now[0] += 30;
                inflater.read(input, length, new byte[8], 8);
                now[0] += 10;
                return new ByteArrayDataInput(new byte[8]);
            }), stats);

            profiled.read(in, 5);

            assertThat(
                "the whole fetch",
                TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.blocks.fetch_time_nanos"),
                equalTo(100L)
            );
            assertThat(
                "the inflation inside it",
                TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.blocks.decompress_time_nanos"),
                equalTo(60L)
            );
            assertThat(TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.blocks.fetched"), equalTo(1L));
            assertThat(TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.blocks.decompressed"), equalTo(1L));
        }
    }

    public void testTheBlockReadTimeEndsWhenTheBlockIsAvailable() throws IOException {
        try (Directory dir = new ByteBuffersDirectory(); IndexInput in = input(dir)) {
            final long[] now = { 0 };
            final ProfileCollector stats = TestCollectors.withClock(() -> now[0]);
            final BlockBytesCodec profiled = new ProfiledBlockBytesCodec(codec((input, length) -> {
                now[0] += 15;
                return new ByteArrayDataInput(new byte[4]);
            }), stats);
            final DataInput block = profiled.read(in, 4);
            now[0] += 5_000;
            block.readByte();
            assertThat(
                "decoding afterwards is not part of the fetch",
                TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.blocks.fetch_time_nanos"),
                equalTo(15L)
            );
        }
    }

    public void testAFailedFetchIsTimedButNotCountedAndTheTimerKeepsWorking() throws IOException {
        try (Directory dir = new ByteBuffersDirectory(); IndexInput in = input(dir)) {
            final long[] now = { 0 };
            final int calls = between(3, 40);
            final int failing = between(0, calls - 1);
            final int[] call = { 0 };
            final IOException failure = new IOException("short read");
            final ProfileCollector stats = TestCollectors.withClock(() -> now[0]);
            final BlockBytesCodec profiled = new ProfiledBlockBytesCodec(codec((input, length) -> {
                now[0] += 20;
                if (call[0]++ == failing) {
                    throw failure;
                }
                return new ByteArrayDataInput(new byte[1]);
            }), stats);
            for (int i = 0; i < calls; i++) {
                if (i == failing) {
                    assertThat(expectThrows(IOException.class, () -> profiled.read(in, 1)), sameInstance(failure));
                } else {
                    profiled.read(in, 1);
                }
            }
            assertThat(
                "successful reads only",
                TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.blocks.fetched"),
                equalTo((long) calls - 1)
            );
            assertThat(
                "every attempt is timed, the failed one too",
                TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.blocks.fetch_time_nanos"),
                equalTo(20L * calls)
            );
        }
    }
}
