/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.benchmark.esql;

import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.xpack.esql.datasource.csv.CsvFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * Measures the overhead of error policy checking in CSV parsing.
 * Compares strict (no error tolerance) vs lenient (with error budget)
 * on both clean data (happy path) and data with errors.
 */
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class CsvErrorPolicyBenchmark {

    @Param({ "1000", "10000" })
    int rowCount;

    @Param({ "0.0", "0.01", "0.05" })
    double errorFraction;

    private BlockFactory blockFactory;
    private byte[] csvData;

    @Setup(Level.Trial)
    public void setup() {
        Utils.configureBenchmarkLogging();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("bench")).build();
        csvData = generateCsv(rowCount, errorFraction);
    }

    @Benchmark
    public int strictPolicy() throws IOException {
        return readAll(ErrorPolicy.STRICT);
    }

    @Benchmark
    public int lenientMaxErrors() throws IOException {
        return readAll(new ErrorPolicy(Long.MAX_VALUE, false));
    }

    @Benchmark
    public int lenientMaxErrorsWithRatio() throws IOException {
        return readAll(new ErrorPolicy(Long.MAX_VALUE, 1.0, false));
    }

    @Benchmark
    public int lenientWithLogging() throws IOException {
        return readAll(new ErrorPolicy(Long.MAX_VALUE, true));
    }

    @Benchmark
    public int nullFieldMode() throws IOException {
        return readAll(new ErrorPolicy(ErrorPolicy.Mode.NULL_FIELD, Long.MAX_VALUE, 1.0, false));
    }

    @Benchmark
    public int nullFieldModeWithLogging() throws IOException {
        return readAll(new ErrorPolicy(ErrorPolicy.Mode.NULL_FIELD, Long.MAX_VALUE, 1.0, true));
    }

    private int readAll(ErrorPolicy policy) throws IOException {
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        StorageObject obj = createStorageObject(csvData);
        int totalRows = 0;
        try (CloseableIterator<Page> iter = reader.read(obj, FormatReadContext.builder().batchSize(1000).errorPolicy(policy).build())) {
            while (iter.hasNext()) {
                totalRows += iter.next().getPositionCount();
            }
        } catch (Exception e) {
            if (policy.isStrict() && errorFraction > 0) {
                return -1;
            }
            throw e;
        }
        return totalRows;
    }

    private static byte[] generateCsv(int rows, double errorFraction) {
        StringBuilder sb = new StringBuilder(rows * 30);
        sb.append("id:long,name:keyword,score:double\n");
        int errorInterval = errorFraction > 0 ? (int) (1.0 / errorFraction) : Integer.MAX_VALUE;
        for (int i = 1; i <= rows; i++) {
            if (errorInterval < Integer.MAX_VALUE && i % errorInterval == 0) {
                sb.append("bad_id,Name").append(i).append(",").append(i * 1.5).append("\n");
            } else {
                sb.append(i).append(",Name").append(i).append(",").append(i * 1.5).append("\n");
            }
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static StorageObject createStorageObject(byte[] data) {
        return new StorageObject() {
            @Override
            public InputStream newStream() {
                return new ByteArrayInputStream(data);
            }

            @Override
            public InputStream newStream(long position, long length) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long length() {
                return data.length;
            }

            @Override
            public Instant lastModified() {
                return Instant.now();
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://bench.csv");
            }
        };
    }
}
