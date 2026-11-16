/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.cache.FooterByteCache;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractTestStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.DeclaredTypeCoercions;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.StorageIdentity;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Arrays;
import java.util.Locale;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

/**
 * SCRATCH MEASUREMENT HARNESS — not a correctness test, not for check-in.
 *
 * <p>Bounds what hoisting the declared-type coercion out of {@link PageColumnReader#readBatchSparse}'s
 * per-run loop could recover. Three arms over the same fixture and the same survivor set:
 * <ul>
 *   <li><b>A (defused)</b> — {@code fileEsqlType=LONG, esqlType=INTEGER}: the pair is not
 *       {@code fusedInDecode} and is {@code supports}ed, so every run pays a {@code castBlock}.</li>
 *   <li><b>B (control)</b> — {@code LONG -> LONG}: no conversion anywhere. This is the decode +
 *       skip + concat skeleton the hoisted design keeps unchanged.</li>
 *   <li><b>C (one cast)</b> — one {@code castBlock(LONG -> INTEGER)} over the assembled block arm B
 *       produced. This is the single conversion the hoisted design would add back.</li>
 * </ul>
 * So {@code B + C} estimates the hoisted time without implementing it, {@code A - B} is the
 * gross gap, and {@code A - (B + C)} is the hoist's entitlement.
 */
public class SparseCastHoistBoundTests extends ESTestCase {

    private static final MessageType SCHEMA = Types.buildMessage().required(INT64).named("v").named("sparse_cast_bound");

    /** One row group, large enough that the general sparse path has many runs to walk. */
    private static final int ROWS = 400_000;

    private static final int WARMUP = 25;
    private static final int MEASURE = 50;

    private final FooterByteCache footerByteCache = FooterByteCache.fromSettings(Settings.EMPTY);
    private BlockFactory blockFactory;
    private PlainCompressionCodecFactory codecFactory;

    @Before
    public void setUpFactories() {
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(NoopCircuitBreaker.INSTANCE).build();
        codecFactory = new PlainCompressionCodecFactory();
    }

    @After
    public void releaseCodecFactory() {
        codecFactory.release();
    }

    public void testBoundTheHoist() throws IOException {
        byte[] data = writeFixture();

        // Confirm the fixture is one row group of ROWS rows: the survivor coordinate space the
        // sparse loop walks is a single row group's.
        try (ParquetFileReader reader = openReader(data)) {
            assertEquals(1, reader.getRowGroups().size());
            assertEquals(ROWS, reader.getRowGroups().getFirst().getRowCount());
        }

        Pattern[] patterns = new Pattern[] {
            pattern("isolated-1k", 1_000, 1),
            pattern("isolated-10k", 10_000, 1),
            pattern("clustered-1k", 1_000, 100),
            pattern("clustered-10k", 10_000, 1_000), };

        StringBuilder report = new StringBuilder("\n==== SPARSE CAST HOIST BOUND (rows=" + ROWS + ") ====\n");
        for (Pattern p : patterns) {
            // The general path is taken iff survivorCount is neither 0 nor sourceRows and the
            // survivors are not one contiguous run: readBatchSparse's own three guards.
            int count = p.positions().length;
            assertTrue("survivorCount must be > 0", count > 0);
            assertTrue("survivorCount must be < sourceRows", count < ROWS);
            assertTrue(
                "survivors must not be one contiguous run, or the fast path short-circuits",
                p.positions()[count - 1] - p.positions()[0] != count - 1
            );
            assertEquals("run count", p.runs(), runCount(p.positions()));

            long[] a = new long[MEASURE];
            long[] b = new long[MEASURE];
            long[] c = new long[MEASURE];
            for (int i = 0; i < WARMUP + MEASURE; i++) {
                int slot = i - WARMUP;
                // Interleave the arms so drift hits all three alike.
                long ta = timeSparse(data, DataType.INTEGER, DataType.LONG, p.positions(), null);
                long[] bc = new long[1];
                long tb = timeSparse(data, DataType.LONG, DataType.LONG, p.positions(), bc);
                if (slot >= 0) {
                    a[slot] = ta;
                    b[slot] = tb;
                    c[slot] = bc[0];
                }
            }
            double medA = medianMicros(a);
            double medB = medianMicros(b);
            double medC = medianMicros(c);
            report.append(
                String.format(
                    Locale.ROOT,
                    "%-14s survivors=%6d runs=%6d  A(defused)=%9.1fus  B(no-cast)=%9.1fus  C(one cast)=%9.1fus"
                        + "  gross gap A-B=%9.1fus (%5.1f%% of A)  entitlement A-(B+C)=%9.1fus (%5.1f%% of A)\n",
                    p.name(),
                    count,
                    p.runs(),
                    medA,
                    medB,
                    medC,
                    medA - medB,
                    100.0 * (medA - medB) / medA,
                    medA - medB - medC,
                    100.0 * (medA - medB - medC) / medA
                )
            );
        }
        logger.info(report.toString());
    }

    /**
     * One timed {@code readBatchSparse} over a fresh reader. Everything but the call itself — opening
     * the file, reading the row group into memory, constructing the reader — is outside the timed
     * region. When {@code castNanos} is non-null the returned block is additionally cast once
     * ({@code LONG -> INTEGER}) and that cast is timed into {@code castNanos[0]}.
     */
    private long timeSparse(byte[] data, DataType esqlType, DataType fileEsqlType, int[] survivors, long[] castNanos) throws IOException {
        try (ParquetFileReader reader = openReader(data)) {
            BlockMetaData rg = reader.getRowGroups().getFirst();
            ColumnDescriptor desc = reader.getFileMetaData().getSchema().getColumnDescription(new String[] { "v" });
            ColumnInfo info = new ColumnInfo(
                desc,
                desc.getPrimitiveType().getPrimitiveTypeName(),
                esqlType,
                desc.getMaxDefinitionLevel(),
                desc.getMaxRepetitionLevel(),
                desc.getPrimitiveType().getLogicalTypeAnnotation(),
                null,
                fileEsqlType
            );
            PageReadStore store = reader.readNextRowGroup();
            assertNotNull(store);
            int rgRows = Math.toIntExact(rg.getRowCount());
            try (PageColumnReader pcr = new PageColumnReader(store.getPageReader(desc), desc, info, RowRanges.all(rgRows))) {
                long start = System.nanoTime();
                Block out = pcr.readBatchSparse(rgRows, blockFactory, survivors, survivors.length);
                long elapsed = System.nanoTime() - start;
                try {
                    assertEquals(survivors.length, out.getPositionCount());
                    if (esqlType == DataType.INTEGER) {
                        // The cast really ran: the block is an IntBlock carrying the source row's value.
                        IntBlock ib = (IntBlock) out;
                        assertEquals(survivors[0], ib.getInt(ib.getFirstValueIndex(0)));
                    } else {
                        LongBlock lb = (LongBlock) out;
                        assertEquals((long) survivors[0], lb.getLong(lb.getFirstValueIndex(0)));
                    }
                    if (castNanos != null) {
                        long castStart = System.nanoTime();
                        Block cast = DeclaredTypeCoercions.castBlock(
                            out,
                            DataType.LONG,
                            DataType.INTEGER,
                            null,
                            blockFactory,
                            "v",
                            null,
                            null
                        );
                        castNanos[0] = System.nanoTime() - castStart;
                        assertEquals(survivors.length, cast.getPositionCount());
                        cast.close();
                    }
                } finally {
                    out.close();
                }
                return elapsed;
            }
        }
    }

    /**
     * {@code survivors} survivors in {@code runs} equal contiguous runs, spread evenly across the row
     * group. {@code runLength == 1} gives the all-isolated shape (one run per survivor).
     */
    private static Pattern pattern(String name, int survivors, int runLength) {
        int runs = survivors / runLength;
        int stride = ROWS / runs;
        assert stride > runLength : "runs would touch and merge";
        int[] positions = new int[survivors];
        int at = 0;
        for (int r = 0; r < runs; r++) {
            int base = r * stride;
            for (int k = 0; k < runLength; k++) {
                positions[at++] = base + k;
            }
        }
        return new Pattern(name, positions, runs);
    }

    private record Pattern(String name, int[] positions, int runs) {}

    /** The same contiguous-run decomposition {@code readBatchSparse}'s general loop performs. */
    private static int runCount(int[] positions) {
        int runs = 1;
        for (int i = 1; i < positions.length; i++) {
            if (positions[i] != positions[i - 1] + 1) {
                runs++;
            }
        }
        return runs;
    }

    private static double medianMicros(long[] nanos) {
        long[] copy = Arrays.copyOf(nanos, nanos.length);
        Arrays.sort(copy);
        return copy[copy.length / 2] / 1000.0;
    }

    private ParquetFileReader openReader(byte[] data) throws IOException {
        return ParquetFileReader.open(
            new ParquetStorageObjectAdapter(storageObject(data), footerByteCache, blockFactory.breaker()),
            PlainParquetReadOptions.builder(codecFactory).build()
        );
    }

    /**
     * One row group, one required INT64 column whose value is its source-row index (so every value
     * fits an {@code integer} and the coercion never fails), uncompressed, dictionary off — PLAIN
     * encoding, the cheapest INT64 decode there is, so the conversion's share of the read is an
     * upper bound rather than a typical one.
     */
    private byte[] writeFixture() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(SCHEMA);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile(out))
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(codecFactory)
                .withType(SCHEMA)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withDictionaryEncoding(false)
                .withRowGroupSize(256L * 1024 * 1024)
                .withPageSize(1024 * 1024)
                .build()
        ) {
            for (int i = 0; i < ROWS; i++) {
                Group g = factory.newGroup();
                g.append("v", (long) i);
                writer.write(g);
            }
        }
        return out.toByteArray();
    }

    private static StorageObject storageObject(byte[] data) {
        return new StorageObject() {
            @Override
            public void attachMetrics(ExternalSourceMetrics metrics, String scheme) {}

            @Override
            public String version() {
                return "v1";
            }

            @Override
            public StorageIdentity storageIdentity() {
                return AbstractTestStorageObject.NOOP;
            }

            @Override
            public InputStream newStream() {
                return new ByteArrayInputStream(data);
            }

            @Override
            public InputStream newStream(long position, long length) {
                return new ByteArrayInputStream(data, (int) position, (int) Math.min(length, data.length - position));
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
                return StoragePath.of("memory://sparse_cast_bound.parquet");
            }
        };
    }

    private static OutputFile outputFile(ByteArrayOutputStream out) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    private long position = 0;

                    @Override
                    public long getPos() {
                        return position;
                    }

                    @Override
                    public void write(int b) {
                        out.write(b);
                        position++;
                    }

                    @Override
                    public void write(byte[] b, int off, int len) {
                        out.write(b, off, len);
                        position += len;
                    }
                };
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) {
                return create(blockSizeHint);
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
}
