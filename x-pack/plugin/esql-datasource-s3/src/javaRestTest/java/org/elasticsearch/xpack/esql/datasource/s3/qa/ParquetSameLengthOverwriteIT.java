/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3.qa;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.datasources.DatasetRegistry;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.DataSourcesS3HttpFixture;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runners.model.Statement;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.ACCESS_KEY;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.BUCKET;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.SECRET_KEY;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.WAREHOUSE;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.addBlobToFixture;

/**
 * A Parquet object overwritten in place with a file of the same byte length must be read as the new file. The footer
 * caches are keyed on {@code (storage identity, path, length, version)}, so a query after the overwrite must not answer from the
 * footer of the first file: neither the statistics for {@code COUNT(*)}, {@code MIN} and {@code MAX}, nor the rows of a
 * file small enough for the footer prefetch. See elastic/esql-planning#2094.
 *
 * <p>The small fixtures have 100 rows and the large ones 10000; the second file's {@code v} values start at 1000000. Each
 * test uses its own object key and dataset, so the caches of one test cannot answer for the other.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class ParquetSameLengthOverwriteIT extends ESRestTestCase {

    private static final String ENCRYPTION_PASSWORD_ID = "test";
    private static final String DATA_SOURCE = "same_length_ds";

    private static final String KEY_STATS = WAREHOUSE + "/same_length/stats.parquet";
    private static final String DATASET_STATS = "same_length_stats";

    // The rows test reads through a glob: with a single-file dataset the rows were not served stale.
    private static final String KEY_ROWS = WAREHOUSE + "/same_length/rows/part-1.parquet";
    private static final String GLOB_ROWS = WAREHOUSE + "/same_length/rows/*.parquet";
    private static final String DATASET_ROWS = "same_length_rows";

    // Larger than the 64 KiB footer prefetch, so the statistics come from the footer and not from a whole-file read.
    private static final String GLOB_LARGE = WAREHOUSE + "/same_length/large/*.parquet";
    private static final String KEY_LARGE = WAREHOUSE + "/same_length/large/part-1.parquet";
    private static final String DATASET_LARGE = "same_length_large";

    // The control: the object is overwritten with a file one row longer, which must read as the new file as well.
    private static final String KEY_LONGER = WAREHOUSE + "/same_length/longer.parquet";
    private static final String DATASET_LONGER = "same_length_longer";

    private static final String V1 = "/same_length_overwrite/v1.parquet";
    private static final String V2 = "/same_length_overwrite/v2.parquet";
    private static final String LARGE_V1 = "/same_length_overwrite/large_v1.parquet";
    private static final String LARGE_V2 = "/same_length_overwrite/large_v2.parquet";
    private static final String LONGER_V2 = "/same_length_overwrite_longer_v2.parquet";

    static DataSourcesS3HttpFixture fixture = new DataSourcesS3HttpFixture();

    private static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting(S3FixtureUtils.ALLOWED_ENDPOINT_HOSTS_SETTING, S3FixtureUtils.LOOPBACK_ENDPOINT_HOSTS)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting(Federation.FEDERATION_ENABLED.getKey(), "true")
        // The listing and file-metadata caches hold the version for this long. The overwrite waits past it, so the
        // second query sees the new version, as it would after the listing TTL in production.
        .setting("esql.external.cache.listing.ttl", "1s")
        .keystore("cluster.state.encryption.password." + ENCRYPTION_PASSWORD_ID, "same-length-overwrite-password")
        .keystore("cluster.state.encryption.active_password_id", ENCRYPTION_PASSWORD_ID)
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule((base, description) -> new Statement() {
        @Override
        public void evaluate() throws Throwable {
            assumeFalse("FIPS mode requires security enabled; this test uses plain HTTP S3 fixtures", inFipsJvm());
            assumeTrue("FROM <dataset> over external data sources required", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());
            base.evaluate();
        }
    }).around(fixture).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @AfterClass
    public static void cleanup() throws IOException {
        try {
            DatasetRegistry.cleanup(client());
        } finally {
            DatasetRegistry.clearCaches();
        }
    }

    /** {@code COUNT(*)}, {@code MIN} and {@code MAX} after an in-place same-length overwrite must come from the new file. */
    public void testStatisticsAfterSameLengthOverwriteComeFromNewFile() throws Exception {
        register(DATASET_STATS, KEY_STATS);
        addBlobToFixture(fixture.getHandler(), KEY_STATS, resource(V1));
        String stats = "FROM " + DATASET_STATS + " | STATS c = COUNT(*), mn = MIN(v), mx = MAX(v)";
        assertEquals(List.of(List.of(100L, 0L, 99L)), query(stats));

        overwriteAfterModificationTimeMoves(KEY_STATS, V2);
        assertEquals(
            "the object was overwritten with a file of the same length; the statistics must come from the new file",
            List.of(List.of(100L, 1_000_000L, 1_000_099L)),
            query(stats)
        );
    }

    /**
     * {@code COUNT(*)}, {@code MIN} and {@code MAX} of a file above the footer prefetch, read through a glob, must come from the
     * new file after a same-length overwrite. The glob is the production shape the issue reports.
     */
    public void testStatisticsOfLargeFileAfterSameLengthOverwriteComeFromNewFile() throws Exception {
        register(DATASET_LARGE, GLOB_LARGE);
        addBlobToFixture(fixture.getHandler(), KEY_LARGE, resource(LARGE_V1));
        String stats = "FROM " + DATASET_LARGE + " | STATS c = COUNT(*), mn = MIN(v), mx = MAX(v)";
        assertEquals(List.of(List.of(10_000L, 0L, 9_999L)), query(stats));

        overwriteAfterModificationTimeMoves(KEY_LARGE, LARGE_V2);
        assertEquals(
            "the object was overwritten with a file of the same length; the statistics must come from the new file",
            List.of(List.of(10_000L, 1_000_000L, 1_009_999L)),
            query(stats)
        );
    }

    /**
     * Control: an overwrite with a file one row longer is read as the new file. The length is part of the footer key, so this
     * case must pass on a build without the fix as well.
     */
    public void testDifferentLengthOverwriteComesFromNewFile() throws Exception {
        register(DATASET_LONGER, KEY_LONGER);
        addBlobToFixture(fixture.getHandler(), KEY_LONGER, resource(LARGE_V1));
        String stats = "FROM " + DATASET_LONGER + " | STATS c = COUNT(*), mn = MIN(v), mx = MAX(v)";
        assertEquals(List.of(List.of(10_000L, 0L, 9_999L)), query(stats));

        overwriteAfterModificationTimeMoves(KEY_LONGER, LONGER_V2);
        assertEquals(List.of(List.of(10_001L, 1_000_000L, 1_010_000L)), query(stats));
    }

    /** The rows of a file small enough for the footer prefetch must come from the new file after a same-length overwrite. */
    public void testRowsAfterSameLengthOverwriteComeFromNewFile() throws Exception {
        register(DATASET_ROWS, GLOB_ROWS);
        addBlobToFixture(fixture.getHandler(), KEY_ROWS, resource(V1));
        String firstRow = "FROM " + DATASET_ROWS + " | SORT v | KEEP v | LIMIT 1";
        assertEquals(List.of(List.of(0L)), query(firstRow));

        overwriteAfterModificationTimeMoves(KEY_ROWS, V2);
        assertEquals(
            "the object was overwritten with a file of the same length; the rows must come from the new file",
            List.of(List.of(1_000_000L)),
            query(firstRow)
        );
    }

    private static void register(String dataset, String resourcePath) throws IOException {
        DatasetRegistry.ensureDataSource(
            client(),
            DATA_SOURCE,
            "s3",
            Map.of("endpoint", fixture.getAddress(), "access_key", ACCESS_KEY, "secret_key", SECRET_KEY)
        );
        DatasetRegistry.ensureDataset(client(), dataset, DATA_SOURCE, "s3://" + BUCKET + "/" + resourcePath, null);
    }

    /** Moves the object's modification time forward, as a real rewrite a moment later would. */
    private static void overwriteAfterModificationTimeMoves(String key, String newVersion) throws IOException {
        safeSleep(1_100);
        addBlobToFixture(fixture.getHandler(), key, resource(newVersion));
    }

    private static byte[] resource(String name) throws IOException {
        try (InputStream in = ParquetSameLengthOverwriteIT.class.getResourceAsStream(name)) {
            assertNotNull("missing test resource " + name, in);
            return in.readAllBytes();
        }
    }

    @SuppressWarnings("unchecked")
    private List<List<Long>> query(String esql) throws IOException {
        RestEsqlTestCase.RequestObjectBuilder req = new RestEsqlTestCase.RequestObjectBuilder().query(esql).pragmasOk();
        Map<String, Object> result = RestEsqlTestCase.runEsqlSync(req, new AssertWarnings.NoWarnings(), null);
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertNotNull("Expected values in the ES|QL result", values);
        List<List<Long>> rows = new ArrayList<>();
        for (List<Object> row : values) {
            List<Long> longs = new ArrayList<>();
            for (Object cell : row) {
                longs.add(((Number) cell).longValue());
            }
            rows.add(longs);
        }
        return rows;
    }
}
