/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.benchmark._nightly.esql;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;

/**
 * Common scaffolding shared by the format-reader JMH benchmarks
 * ({@link ParquetReadBenchmark}, {@link OrcReadBenchmark},
 * {@link NdJsonReadBenchmark}, {@link CsvReadBenchmark},
 * {@link CrossFormatReadBenchmark}). Mirrors the in-memory {@code StorageObject}
 * pattern that {@code ParallelParsingBenchmark} and {@code CsvErrorPolicyBenchmark}
 * use inline, extracted into one place so the five new benches don't each carry
 * their own copy.
 *
 * <p>Also exposes {@link #SELF_TEST_ROW_COUNT}: a small fixture size used by every
 * {@code selfTest()} so the per-PR JUnit smoke tests stay fast even though the JMH
 * runs use 10k/100k rows.
 */
final class DatasourceBenchmarks {

    private DatasourceBenchmarks() {}

    /**
     * Row count used by every {@code selfTest()} in this package. Small enough that
     * generating five format fixtures per param combo stays under a second total,
     * large enough to exceed Parquet's default page size and the various readers'
     * batch boundaries (1000 batch size) so each path runs the multi-batch loop at
     * least once.
     */
    static final int SELF_TEST_ROW_COUNT = 2000;

    /**
     * Returns a {@link BlockFactory} suitable for JMH measurement: no recycling, no
     * circuit-breaker accounting, no allocation overhead beyond {@code byte[]} allocations
     * for the produced blocks. Matches the setup used by the existing CSV benchmarks.
     */
    static BlockFactory newBlockFactory() {
        return BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("bench")).build();
    }

    /**
     * Wraps a {@code byte[]} as a {@link StorageObject}. Supports both full and ranged
     * reads (Parquet/ORC use the ranged form to seek to the footer), and the positional
     * {@link StorageObject#readBytes(long, ByteBuffer)} fast path.
     */
    static StorageObject inMemoryStorageObject(byte[] data, String uri) {
        return new InMemoryStorageObject(data, uri);
    }

    private static final class InMemoryStorageObject implements StorageObject {
        private final byte[] data;
        private final String uri;

        InMemoryStorageObject(byte[] data, String uri) {
            this.data = data;
            this.uri = uri;
        }

        @Override
        public InputStream newStream() {
            return new ByteArrayInputStream(data);
        }

        @Override
        public InputStream newStream(long position, long length) {
            if (position >= data.length) {
                return new ByteArrayInputStream(data, 0, 0);
            }
            int pos = Math.toIntExact(position);
            int len = (int) Math.min(length, data.length - position);
            return new ByteArrayInputStream(data, pos, len);
        }

        @Override
        public int readBytes(long position, ByteBuffer target) {
            if (position >= data.length) {
                return -1;
            }
            int pos = Math.toIntExact(position);
            int len = Math.min(target.remaining(), data.length - pos);
            target.put(data, pos, len);
            return len;
        }

        @Override
        public long length() {
            return data.length;
        }

        @Override
        public Instant lastModified() {
            return Instant.EPOCH;
        }

        @Override
        public boolean exists() {
            return true;
        }

        @Override
        public StoragePath path() {
            return StoragePath.of(uri);
        }
    }
}
