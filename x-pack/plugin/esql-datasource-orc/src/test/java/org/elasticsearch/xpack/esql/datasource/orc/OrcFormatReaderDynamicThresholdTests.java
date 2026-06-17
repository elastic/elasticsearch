/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.compute.operator.topn.SharedNumericThreshold;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.DynamicThreshold;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

public class OrcFormatReaderDynamicThresholdTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        OrcStorageObjectAdapter.clearCacheForTests();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    public void testStripeSkipLeavesOnlyNonDominatedStripe() throws Exception {
        byte[] data = createMultiStripeOrcFile(3, (stripeIndex, batch) -> {
            batch.size = 100;
            LongColumnVector id = (LongColumnVector) batch.cols[0];
            long base = stripeIndex * 1_000L;
            for (int i = 0; i < batch.size; i++) {
                id.vector[i] = base + i;
            }
        });

        List<Long> rows = readIdsWithThreshold(data, threshold(99L, true, false));

        assertThat(rows.size(), equalTo(100));
        assertTrue(rows.contains(0L));
        assertTrue(rows.contains(99L));
        assertFalse(rows.contains(1_000L));
    }

    public void testNoFurtherCandidatesExhaustsImmediately() throws Exception {
        byte[] data = createMultiStripeOrcFile(2, (stripeIndex, batch) -> {
            batch.size = 100;
            LongColumnVector id = (LongColumnVector) batch.cols[0];
            for (int i = 0; i < batch.size; i++) {
                id.vector[i] = stripeIndex * 1_000L + i;
            }
        });
        SharedNumericThreshold.Supplier supplier = new SharedNumericThreshold.Supplier(true, true);
        SharedNumericThreshold channel = supplier.get();
        channel.markNoFurtherCandidates();

        List<Long> rows = readIdsWithThreshold(data, new DynamicThreshold("id", ElementType.LONG, true, true, channel));

        assertThat(rows.size(), equalTo(0));
    }

    public void testNullsLastSkipsDominatedStripeWithNullsPresent() throws Exception {
        byte[] data = createMultiStripeOrcFile(2, (stripeIndex, batch) -> {
            batch.size = 100;
            LongColumnVector id = (LongColumnVector) batch.cols[0];
            id.noNulls = false;
            long base = stripeIndex * 1_000L;
            for (int i = 0; i < batch.size; i++) {
                if (i % 10 == 0) {
                    id.isNull[i] = true;
                } else {
                    id.vector[i] = base + i;
                }
            }
        });

        List<Long> rows = readIdsWithThreshold(data, threshold(99L, true, false));

        assertThat(rows.size(), lessThan(200));
        assertTrue(rows.contains(null));
        assertTrue(rows.contains(99L));
        assertFalse(rows.contains(1_001L));
    }

    public void testNullsFirstKeepsDominatedStripeWithNullsPresent() throws Exception {
        byte[] data = createMultiStripeOrcFile(2, (stripeIndex, batch) -> {
            batch.size = 100;
            LongColumnVector id = (LongColumnVector) batch.cols[0];
            id.noNulls = false;
            long base = stripeIndex * 1_000L;
            for (int i = 0; i < batch.size; i++) {
                if (stripeIndex == 1 && i % 10 == 0) {
                    id.isNull[i] = true;
                } else {
                    id.vector[i] = base + i;
                }
            }
        });

        List<Long> rows = readIdsWithThreshold(data, threshold(99L, true, true));

        assertThat(rows.size(), equalTo(200));
        assertTrue(rows.contains(null));
        assertTrue(rows.contains(1_001L));
    }

    private DynamicThreshold threshold(long value, boolean ascending, boolean nullsFirst) {
        SharedNumericThreshold.Supplier supplier = new SharedNumericThreshold.Supplier(ascending, nullsFirst);
        SharedNumericThreshold channel = supplier.get();
        channel.offer(value);
        return new DynamicThreshold("id", ElementType.LONG, ascending, nullsFirst, channel);
    }

    private List<Long> readIdsWithThreshold(byte[] data, DynamicThreshold threshold) throws IOException {
        OrcFormatReader reader = (OrcFormatReader) new OrcFormatReader(blockFactory).withDynamicThreshold(threshold);
        try (threshold; CloseableIterator<Page> iterator = reader.read(storageObject(data), List.of("id"), 128)) {
            List<Long> values = new ArrayList<>();
            while (iterator.hasNext()) {
                Page page = iterator.next();
                try {
                    LongBlock block = page.getBlock(0);
                    for (int p = 0; p < block.getPositionCount(); p++) {
                        values.add(block.isNull(p) ? null : block.getLong(block.getFirstValueIndex(p)));
                    }
                } finally {
                    page.releaseBlocks();
                }
            }
            return values;
        }
    }

    private byte[] createMultiStripeOrcFile(int stripeCount, StripePopulator populator) throws IOException {
        TypeDescription schema = TypeDescription.createStruct().addField("id", TypeDescription.createLong());
        var tempFile = createTempFile();
        Files.delete(tempFile);
        Path orcPath = new Path(tempFile.toUri());

        Configuration conf = new Configuration(false);
        conf.set("orc.key.provider", "memory");
        NoPermissionLocalFileSystem localFs = new NoPermissionLocalFileSystem();
        localFs.setConf(conf);
        OrcFile.WriterOptions writerOptions = OrcFile.writerOptions(conf)
            .setSchema(schema)
            .fileSystem(localFs)
            .compress(CompressionKind.NONE)
            .stripeSize(512);

        try (Writer writer = OrcFile.createWriter(orcPath, writerOptions)) {
            for (int s = 0; s < stripeCount; s++) {
                VectorizedRowBatch batch = schema.createRowBatch();
                populator.populate(s, batch);
                writer.addRowBatch(batch);
                writer.writeIntermediateFooter();
            }
        }

        return Files.readAllBytes(tempFile);
    }

    private static StorageObject storageObject(byte[] data) {
        return new StorageObject() {
            @Override
            public InputStream newStream() {
                return new ByteArrayInputStream(data);
            }

            @Override
            public InputStream newStream(long position, long length) {
                int pos = (int) position;
                int len = (int) Math.min(length, data.length - position);
                return new ByteArrayInputStream(data, pos, len);
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
                return StoragePath.of("memory://dynamic-threshold.orc");
            }
        };
    }

    @FunctionalInterface
    private interface StripePopulator {
        void populate(int stripeIndex, VectorizedRowBatch batch);
    }

    private static class NoPermissionLocalFileSystem extends RawLocalFileSystem {
        @Override
        @SuppressForbidden(reason = "Bypass Hadoop's LocalFSFileOutputStream to avoid Shell.<clinit>")
        protected OutputStream createOutputStreamWithMode(Path p, boolean append, FsPermission permission) throws IOException {
            return new FileOutputStream(pathToFile(p), append);
        }

        @Override
        public void setPermission(Path p, FsPermission permission) {
            // no-op: skip chmod calls that would trigger Shell
        }

        @Override
        @SuppressForbidden(reason = "Hadoop API requires java.io.File in method signature")
        protected boolean mkOneDirWithMode(Path p, File p2f, FsPermission permission) throws IOException {
            return p2f.mkdir() || p2f.isDirectory();
        }
    }
}
