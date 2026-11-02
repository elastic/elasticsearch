/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedDocValues;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesRoutingHashFieldMapper;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.Before;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Checks that a time series index opted into {@link IndexSettings#TIME_SERIES_RUN_TABLE_ORDINALS_ENABLED_SETTING}
 * writes its doc values with {@link ES95RunTableTSDBDocValuesFormat}, end to end through the index's codec.
 */
public class ES95RunTableTSDBDocValuesFormatSingleNodeTests extends ES95TSDBDocValuesFormatSingleNodeTests {

    @Before
    public void assumeFeatureFlagEnabled() {
        assumeTrue(
            "tsdb_run_table_ordinals feature flag must be enabled",
            IndexSettings.TIME_SERIES_RUN_TABLE_ORDINALS_FEATURE_FLAG.isEnabled()
        );
    }

    /**
     * Through the real mapping and codec wiring, single-valued and multi-valued dimensions and
     * {@code _ts_routing_hash} are written as run tables, while {@code _tsid}, the primary sort field, keeps its
     * own layout.
     */
    public void testDimensionsAreWrittenAsRunTables() throws Exception {
        final String indexName = "run-table";
        createIndex(
            indexName,
            tsdbSettings(),
            "@timestamp",
            "type=date",
            "hostname",
            "type=keyword,time_series_dimension=true",
            "region",
            "type=keyword,time_series_dimension=true",
            "tags",
            "type=keyword,time_series_dimension=true",
            "gauge",
            "type=long,time_series_metric=gauge"
        );
        final long baseTimestamp = 1704067200000L;
        final int numHosts = randomIntBetween(4, 10);
        final int docsPerHost = randomIntBetween(20, 50);
        final BulkRequestBuilder bulk = client().prepareBulk();
        for (int host = 0; host < numHosts; host++) {
            for (int i = 0; i < docsPerHost; i++) {
                bulk.add(
                    prepareIndex(indexName).setSource(
                        XContentFactory.jsonBuilder()
                            .startObject()
                            .field("@timestamp", baseTimestamp + i * 1000L)
                            .field("hostname", "host-" + host)
                            .field("region", "region-" + (host % 2))
                            .array("tags", "tag-" + (host % 3), "tag-z")
                            .field("gauge", randomLong())
                            .endObject()
                    )
                );
            }
        }
        assertFalse(bulk.get().hasFailures());
        indicesAdmin().flush(new FlushRequest(indexName).force(true)).actionGet();
        indicesAdmin().prepareRefresh(indexName).get();

        try (Engine.Searcher searcher = getShard(indexName).acquireExternalSearcher("test", SplitShardCountSummary.IRRELEVANT)) {
            assertThat(searcher.getLeafContexts().size(), equalTo(1));
            final LeafReader leaf = searcher.getLeafContexts().get(0).reader();
            assertTrue("hostname", ES95RunTableTSDBDocValuesFormatTests.isRunTable(sorted(leaf, "hostname")));
            assertTrue("region", ES95RunTableTSDBDocValuesFormatTests.isRunTable(sorted(leaf, "region")));
            assertTrue("tags", ES95RunTableTSDBDocValuesFormatTests.isRunTable(leaf.getSortedSetDocValues("tags")));
            assertTrue(
                TimeSeriesRoutingHashFieldMapper.NAME,
                ES95RunTableTSDBDocValuesFormatTests.isRunTable(sorted(leaf, TimeSeriesRoutingHashFieldMapper.NAME))
            );
            assertFalse(
                TimeSeriesIdFieldMapper.NAME,
                ES95RunTableTSDBDocValuesFormatTests.isRunTable(sorted(leaf, TimeSeriesIdFieldMapper.NAME))
            );
        }
    }

    /** A field's sorted doc values, whether it is mapped as {@code Sorted} or as single-valued {@code SortedSet}. */
    private static SortedDocValues sorted(final LeafReader leaf, final String field) throws IOException {
        final SortedDocValues sorted = leaf.getSortedDocValues(field);
        return sorted != null ? sorted : DocValues.unwrapSingleton(leaf.getSortedSetDocValues(field));
    }

    @Override
    protected Settings tsdbSettings() {
        return Settings.builder()
            .put(super.tsdbSettings())
            .put(IndexSettings.TIME_SERIES_RUN_TABLE_ORDINALS_ENABLED_SETTING.getKey(), true)
            .build();
    }

    @Override
    protected void assertTSDBDocValuesFormat(final DocValuesFormat format, final String field) {
        assertThat(
            "field [" + field + "] should use the run-table ES95 doc values format",
            format,
            instanceOf(ES95RunTableTSDBDocValuesFormat.class)
        );
    }

    @Override
    protected String expectedCodecName() {
        return ES95RunTableTSDBDocValuesFormat.CODEC_NAME;
    }
}
