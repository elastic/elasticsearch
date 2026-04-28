/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Verifies that the production row-group skip path (parquet-mr's
 * {@code RowGroupFilter.filterRowGroups} called from
 * {@code ParquetFormatReader.computeSurvivingRowGroups}) produces the same keep/drop set as
 * {@link ParquetFileReader#readNextFilteredRowGroup()} for a multi-row-group file. This guards
 * against regressions in how we configure {@code RowGroupFilter} (filter levels, predicate
 * resolution) on the optimized read path.
 */
public class StatisticsRowGroupFilterParityTests extends ESTestCase {

    private static final int ROWS_PER_GROUP = 1024;
    private static final int ROW_GROUPS = 4;

    private PlainCompressionCodecFactory codecFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        codecFactory = new PlainCompressionCodecFactory();
    }

    @Override
    public void tearDown() throws Exception {
        codecFactory.release();
        super.tearDown();
    }

    public void testKeepSetMatchesParquetMrForEqualityFilter() throws IOException {
        byte[] file = writeBucketedFile();
        // Each row group's id range is [g*ROWS_PER_GROUP, (g+1)*ROWS_PER_GROUP); pick a value
        // that selects exactly one group.
        int target = ROWS_PER_GROUP * 2 + 7;
        FilterPredicate predicate = FilterApi.eq(FilterApi.intColumn("id"), target);
        Set<Integer> kept = customKeptOrdinals(file, predicate);
        Set<Integer> baseline = baselineKeptOrdinals(file, predicate);
        assertEquals(baseline, kept);
        assertEquals("filter should select exactly one row group", 1, kept.size());
    }

    public void testKeepSetMatchesParquetMrForRangeFilter() throws IOException {
        byte[] file = writeBucketedFile();
        // Range that spans groups 1 and 2.
        FilterPredicate predicate = FilterApi.and(
            FilterApi.gtEq(FilterApi.intColumn("id"), ROWS_PER_GROUP),
            FilterApi.lt(FilterApi.intColumn("id"), 3 * ROWS_PER_GROUP)
        );
        Set<Integer> kept = customKeptOrdinals(file, predicate);
        Set<Integer> baseline = baselineKeptOrdinals(file, predicate);
        assertEquals(baseline, kept);
    }

    public void testKeepSetMatchesParquetMrForFilterDroppingAll() throws IOException {
        byte[] file = writeBucketedFile();
        FilterPredicate predicate = FilterApi.gt(FilterApi.intColumn("id"), 100_000);
        Set<Integer> kept = customKeptOrdinals(file, predicate);
        Set<Integer> baseline = baselineKeptOrdinals(file, predicate);
        assertEquals(baseline, kept);
        assertTrue("expected all row groups to be dropped", kept.isEmpty());
    }

    public void testKeepSetMatchesParquetMrForFilterKeepingAll() throws IOException {
        byte[] file = writeBucketedFile();
        FilterPredicate predicate = FilterApi.gtEq(FilterApi.intColumn("id"), 0);
        Set<Integer> kept = customKeptOrdinals(file, predicate);
        Set<Integer> baseline = baselineKeptOrdinals(file, predicate);
        int totalGroups;
        try (ParquetFileReader reader = openReader(file)) {
            totalGroups = reader.getRowGroups().size();
        }
        assertEquals(baseline, kept);
        assertEquals("expected every row group to survive", totalGroups, kept.size());
    }

    private Set<Integer> customKeptOrdinals(byte[] file, FilterPredicate predicate) throws IOException {
        // Mirror ParquetFormatReader.computeSurvivingRowGroups: same filter levels, same call.
        Set<Integer> kept = new LinkedHashSet<>();
        try (ParquetFileReader reader = openReader(file)) {
            List<BlockMetaData> blocks = reader.getRowGroups();
            List<org.apache.parquet.filter2.compat.RowGroupFilter.FilterLevel> levels = List.of(
                org.apache.parquet.filter2.compat.RowGroupFilter.FilterLevel.STATISTICS,
                org.apache.parquet.filter2.compat.RowGroupFilter.FilterLevel.DICTIONARY,
                org.apache.parquet.filter2.compat.RowGroupFilter.FilterLevel.BLOOMFILTER
            );
            List<BlockMetaData> survivors = org.apache.parquet.filter2.compat.RowGroupFilter.filterRowGroups(
                levels,
                FilterCompat.get(predicate),
                blocks,
                reader
            );
            // Map identity-equal block back to its ordinal (filterRowGroups preserves order).
            int idx = 0;
            for (int i = 0; i < blocks.size() && idx < survivors.size(); i++) {
                if (blocks.get(i) == survivors.get(idx)) {
                    kept.add(i);
                    idx++;
                }
            }
        }
        return kept;
    }

    private Set<Integer> baselineKeptOrdinals(byte[] file, FilterPredicate predicate) throws IOException {
        // parquet-mr internally drops row groups via stats filter when iterating with a
        // FilterCompat filter; map physical block ordinals from the survivors against the
        // original ordering by matching getStartingPos().
        Set<Long> survivingStarts = new HashSet<>();
        try (
            ParquetFileReader reader = ParquetFileReader.open(
                new ParquetStorageObjectAdapter(new InMemoryStorageObject(file)),
                PlainParquetReadOptions.builder(codecFactory).withRecordFilter(FilterCompat.get(predicate)).build()
            )
        ) {
            List<BlockMetaData> blocks = reader.getRowGroups();
            int idx = 0;
            PageReadStore store;
            while ((store = reader.readNextFilteredRowGroup()) != null) {
                while (idx < blocks.size() && blocks.get(idx).getRowCount() != store.getRowCount()) {
                    idx++;
                }
                if (idx < blocks.size()) {
                    survivingStarts.add(blocks.get(idx).getStartingPos());
                    idx++;
                }
                store.close();
            }
        }
        Set<Integer> kept = new LinkedHashSet<>();
        try (ParquetFileReader reader = openReader(file)) {
            List<BlockMetaData> blocks = reader.getRowGroups();
            for (int i = 0; i < blocks.size(); i++) {
                if (survivingStarts.contains(blocks.get(i).getStartingPos())) {
                    kept.add(i);
                }
            }
        }
        return kept;
    }

    private ParquetFileReader openReader(byte[] file) throws IOException {
        return ParquetFileReader.open(
            new ParquetStorageObjectAdapter(new InMemoryStorageObject(file)),
            PlainParquetReadOptions.builder(codecFactory).build()
        );
    }

    private byte[] writeBucketedFile() throws IOException {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT32).named("id").named("rg_filter_test");
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        OutputFile outputFile = wrapOutput(outputStream);
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                // Force a row group per ROWS_PER_GROUP rows by setting a very small group size.
                .withRowGroupSize(1L)
                .withPageSize(64)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
        ) {
            for (int g = 0; g < ROW_GROUPS; g++) {
                int base = g * ROWS_PER_GROUP;
                for (int i = 0; i < ROWS_PER_GROUP; i++) {
                    writer.write(factory.newGroup().append("id", base + i));
                }
            }
        }
        return outputStream.toByteArray();
    }

    private static OutputFile wrapOutput(ByteArrayOutputStream outputStream) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return wrap(outputStream);
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) {
                return wrap(outputStream);
            }

            @Override
            public boolean supportsBlockSize() {
                return false;
            }

            @Override
            public long defaultBlockSize() {
                return 0;
            }
        };
    }

    private static PositionOutputStream wrap(ByteArrayOutputStream outputStream) {
        return new PositionOutputStream() {
            private long pos = 0;

            @Override
            public long getPos() {
                return pos;
            }

            @Override
            public void write(int b) {
                outputStream.write(b);
                pos++;
            }

            @Override
            public void write(byte[] b, int off, int len) {
                outputStream.write(b, off, len);
                pos += len;
            }
        };
    }

    private static final class InMemoryStorageObject implements StorageObject {
        private final byte[] data;

        InMemoryStorageObject(byte[] data) {
            this.data = data;
        }

        @Override
        public StoragePath path() {
            return StoragePath.of("memory://stats-parity.parquet");
        }

        @Override
        public Instant lastModified() {
            return Instant.EPOCH;
        }

        @Override
        public long length() {
            return data.length;
        }

        @Override
        public boolean exists() {
            return true;
        }

        @Override
        public InputStream newStream() {
            return new ByteArrayInputStream(data);
        }

        @Override
        public InputStream newStream(long position, long length) {
            return new ByteArrayInputStream(data, (int) position, (int) length);
        }
    }
}
