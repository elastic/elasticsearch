/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.parquet;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.test.AzureReactorThreadFilter;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.datasources.AbstractFromDatasetSubqueryRestTestCase;
import org.elasticsearch.xpack.esql.datasources.BackendFixture;
import org.elasticsearch.xpack.esql.datasources.S3BackendFixture;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.DataSourcesS3HttpFixture;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.WAREHOUSE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * End-to-end regression for the two-phase page-skip overshoot in {@code PageColumnReader.skipRows}:
 * when a projection column's leading data page is fully excluded by the phase-1 survivor mask,
 * {@code loadNextPage} jumps the physical cursor to the first surviving page via its
 * {@code firstRowIndex}. If that jump overshoots the {@code skipRows} target and the surplus is
 * discarded, the next skip over-advances into the survivor page and silently drops matching rows,
 * surfacing as an undercounted grouped {@code COUNT(DISTINCT)} / {@code VALUES}.
 *
 * <p>The buggy path is reachable only under two-phase decoding, which the reader gates behind
 * {@code StorageObject.supportsNativeAsync()}. A plain {@code file://} dataset takes the single-phase
 * late-materialization path (projection readers get {@code RowRanges == null}) and cannot trigger it.
 * This test therefore serves the fixture from the in-process S3 fixture (native-async), the same
 * infrastructure the parquet csv-spec suites use, so the query runs through the real planner, the
 * async external source operator and the two-phase iterator end to end.
 *
 * <p>Fixture geometry (the three conditions the bug needs, see {@code PageColumnReader.skipRows}):
 * <ul>
 *   <li>The projection column {@code v} is written with dictionary encoding disabled and a small page
 *       size, so it spans many data pages of a predictable row count.</li>
 *   <li>The predicate column {@code s} carries a single short {@code "hit"} cluster far into the row
 *       group (row {@link #HIT_START}); every other row is {@code "bg"}. {@code WHERE s == "hit"} thus
 *       yields a sparse survivor set (one run) so page filtering engages rather than being discarded.</li>
 *   <li>{@link #HIT_START} sits well past the first projection page and past several row batches, so
 *       draining the fully-filtered leading batches issues more than one {@code skipRows} across the
 *       excluded pages: the first banks the overshoot, a later one would over-advance without the fix.</li>
 * </ul>
 *
 * <p>{@code v} is unique per row, so the surviving group's {@code MV_COUNT(VALUES(v))} must equal the
 * exact cluster size {@link #HIT_COUNT}. A dropped survivor makes it smaller.
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class, AzureReactorThreadFilter.class })
public class ParquetTwoPhasePageSkipCorrectnessIT extends AbstractFromDatasetSubqueryRestTestCase {

    private static final String DATA_SOURCE = "two_phase_skip_s3_ds";
    private static final String DATASET = "two_phase_skip_s3";
    private static final String BLOB_KEY = WAREHOUSE + "/standalone/two_phase_page_skip.parquet";

    /** Total rows in the single row group. */
    private static final int TOTAL_ROWS = 40_000;
    /** First row of the sole {@code "hit"} cluster; deep past the leading (excluded) projection pages. */
    private static final int HIT_START = 30_000;
    /** Size of the {@code "hit"} cluster; the exact expected distinct-value count for the survivor group. */
    private static final int HIT_COUNT = 17;

    /** ~1 KiB of int64 values per page (8 KiB page / 8 bytes), so {@code v} spans ~40 pages. */
    private static final int PAGE_SIZE_BYTES = 8 * 1024;
    /** One row group for the whole file, so all pages live under a single {@code BlockMetaData}. */
    private static final long ROW_GROUP_SIZE_BYTES = 1L << 30;

    public static DataSourcesS3HttpFixture s3Fixture = new DataSourcesS3HttpFixture();
    public static ElasticsearchCluster cluster = Clusters.testClusterWithEncryption(() -> s3Fixture.getAddress());

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3Fixture).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @AfterClass
    public static void cleanupRegistry() throws IOException {
        deleteIgnoringMissing("/_query/dataset/" + DATASET);
        deleteIgnoringMissing("/_query/data_source/" + DATA_SOURCE);
    }

    public void testGroupedDistinctSurvivesExcludedProjectionPageSkip() throws Exception {
        BackendFixture s3Backend = new S3BackendFixture(s3Fixture);
        s3Backend.uploadBlob(BLOB_KEY, twoPhasePageSkipParquetBytes());
        putDataSource(DATA_SOURCE, s3Backend.dataSourceType(), s3Backend.dataSourceSettings());
        putDataset(DATASET, DATA_SOURCE, s3Backend.resourceUri(BLOB_KEY), Map.of());

        // WHERE s == "hit" pushes as a late-materialization predicate (RECHECK), giving a sparse
        // survivor mask; VALUES(v) makes v a projection-only column, which together with the S3
        // (native-async) backend selects the two-phase page-filtered decode path under test.
        String query = "FROM " + DATASET + " | WHERE s == \"hit\" | STATS u = MV_COUNT(VALUES(v)) BY s";

        Map<String, Object> response = runQuery(query);
        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) response.get("values");

        assertThat("exactly one surviving group (s == \"hit\")", values, hasSize(1));
        List<Object> row = values.get(0);
        assertThat("group key must be the cluster value", row.get(1).toString(), equalTo("hit"));
        assertThat(
            "distinct v over survivors must equal the exact cluster size; a smaller value means the "
                + "excluded-page skip dropped surviving rows",
            ((Number) row.get(0)).intValue(),
            equalTo(HIT_COUNT)
        );
    }

    /**
     * Builds the fixture described in the class Javadoc: {@code {s: keyword, v: long}} over
     * {@link #TOTAL_ROWS} rows in one row group, {@code v} unique and dictionary-disabled with a small
     * page size, {@code s} equal to {@code "hit"} only on {@code [HIT_START, HIT_START + HIT_COUNT)}.
     */
    private static byte[] twoPhasePageSkipParquetBytes() throws IOException {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("s")
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("v")
            .named("two_phase_page_skip");

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputFile outputFile = byteArrayOutputFile(baos);
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);

        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withConf(new PlainParquetConfiguration())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                // Plain (non-dictionary) int64 pages so v's page boundaries are a predictable function
                // of the byte page size, guaranteeing many full pages precede the survivor cluster.
                .withDictionaryEncoding("v", false)
                .withPageSize(PAGE_SIZE_BYTES)
                .withRowGroupSize(ROW_GROUP_SIZE_BYTES)
                .build()
        ) {
            for (int i = 0; i < TOTAL_ROWS; i++) {
                Group g = factory.newGroup();
                g.add("s", (i >= HIT_START && i < HIT_START + HIT_COUNT) ? "hit" : "bg");
                g.add("v", (long) i);
                writer.write(g);
            }
        }
        return baos.toByteArray();
    }

    private static OutputFile byteArrayOutputFile(ByteArrayOutputStream baos) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return positionOutputStream(baos);
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

    private static PositionOutputStream positionOutputStream(ByteArrayOutputStream baos) {
        return new PositionOutputStream() {
            private long position = 0;

            @Override
            public long getPos() {
                return position;
            }

            @Override
            public void write(int b) {
                baos.write(b);
                position++;
            }

            @Override
            public void write(byte[] b, int off, int len) {
                baos.write(b, off, len);
                position += len;
            }
        };
    }
}
