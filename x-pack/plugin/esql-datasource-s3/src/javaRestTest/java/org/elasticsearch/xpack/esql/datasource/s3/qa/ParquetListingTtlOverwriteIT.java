/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3.qa;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.client.ResponseException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.datasources.AbstractExternalOverwriteRestTestCase;
import org.elasticsearch.xpack.esql.datasources.BackendFixture;
import org.elasticsearch.xpack.esql.datasources.S3BackendFixture;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.DataSourcesS3HttpFixture;
import org.junit.ClassRule;
import org.junit.rules.TestRule;

import java.util.List;

/**
 * A Parquet object overwritten while its listing is still cached, inside a one-hour listing TTL. Two outcomes are possible, and the
 * cases below check both:
 * <ul>
 *   <li>A query that reads the object under the listed version fails with the retryable 503. The failure drops the listing, so the
 *   client's next attempt lists the object again and reads the new rows.</li>
 *   <li>An aggregate that the dataset aggregate cache answers sends no request, so it returns the listed file's result. That is a
 *   consistent snapshot of the listed version, not old metadata with new bytes.</li>
 * </ul>
 * The schema is strict, so the listing is unbounded and cached; the default first-file-wins listing is a sample, which the resolver
 * never caches. Each case has its own object key and dataset, since the one-hour cache outlives a test.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class ParquetListingTtlOverwriteIT extends AbstractExternalOverwriteRestTestCase {

    /** ES|QL returns at most this many rows when the query has no LIMIT. */
    private static final int DEFAULT_ROW_LIMIT = 1_000;
    private static final String STRICT_SCHEMA = "{\"schema_resolution\": \"strict\"}";

    static final DataSourcesS3HttpFixture fixture = new DataSourcesS3HttpFixture();
    private static final S3BackendFixture backend = new S3BackendFixture(fixture);
    // Nothing here waits for the listing to expire: each overwrite lands while the first query's listing is still cached.
    private static final ElasticsearchCluster cluster = externalDatasetCluster("1h").build();

    @ClassRule
    public static TestRule ruleChain = externalDatasetRules("S3", fixture, cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected BackendFixture backend() {
        return backend;
    }

    /**
     * A row read inside the listing TTL. The read pins the listed version, which the object no longer has, so it is refused with
     * the retryable 503 and the failure drops the listing. The client's next attempt lists the object again and returns the rows
     * of the new file.
     */
    public void testRowReadInsideListingTtlFailsWith503ThenReadsNewRows() throws Exception {
        register("same_length_ttl_rows", key("ttl_rows/*.parquet"), STRICT_SCHEMA);
        upload(key("ttl_rows/part-1.parquet"), LARGE_V1);
        String rows = "FROM same_length_ttl_rows | KEEP v";
        assertEquals(DEFAULT_ROW_LIMIT, query(rows).size());

        upload(key("ttl_rows/part-1.parquet"), LARGE_V2);
        ResponseException failed = expectThrows(ResponseException.class, () -> query(rows));
        assertEquals(
            "a read under the listed version must fail with the retryable object-changed status",
            RestStatus.SERVICE_UNAVAILABLE.getStatus(),
            failed.getResponse().getStatusLine().getStatusCode()
        );

        List<List<Long>> newRows = query(rows);
        assertEquals(DEFAULT_ROW_LIMIT, newRows.size());
        assertEquals(
            "the client's next attempt reads the rows of the new file",
            Long.valueOf(1_000_000L),
            newRows.stream().map(r -> r.get(0)).min(Long::compare).orElseThrow()
        );
    }

    /**
     * An aggregate inside the listing TTL is answered from the cache under the listed version, without reading the object: no
     * request is sent, so no conditional request can fail. The answer is the listed file's, a consistent snapshot.
     */
    public void testAggregateInsideListingTtlAnswersFromTheListedVersion() throws Exception {
        register("same_length_ttl_agg", key("ttl_agg/*.parquet"), STRICT_SCHEMA);
        upload(key("ttl_agg/part-1.parquet"), LARGE_V1);
        String max = "FROM same_length_ttl_agg | STATS m = MAX(v)";
        assertEquals(List.of(List.of(9_999L)), query(max));

        upload(key("ttl_agg/part-1.parquet"), LARGE_V2);
        int requestsBefore = fixture.getHandler().requestLog().size();
        assertEquals(
            "the aggregate is answered from the listed version, which is the snapshot the listing names",
            List.of(List.of(9_999L)),
            query(max)
        );
        assertEquals("the aggregate must not reach the object store", requestsBefore, fixture.getHandler().requestLog().size());
    }
}
