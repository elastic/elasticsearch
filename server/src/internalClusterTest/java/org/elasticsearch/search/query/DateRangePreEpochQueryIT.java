/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.query;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.junit.Assert.assertTrue;

/**
 * Search integration tests for {@code date_range} when bounds lie before the Unix epoch or cross it.
 */
public class DateRangePreEpochQueryIT extends ESSingleNodeTestCase {

    /** Same optional formats as {@link org.elasticsearch.index.mapper.RangeFieldMapperTests}. */
    private static final String DATE_FORMAT = "uuuu-MM-dd HH:mm:ss.SSS||yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis";

    private static final String INDEX = "date_range_pre_epoch_query_it";

    public void testRangeQueryFindsDocumentWithPreEpochRange() throws Exception {
        createDateRangeIndex();
        index("""
            {"dr":{"gte":"1969-06-01 00:00:00.000","lte":"1969-08-01 00:00:00.000"}}""");
        index("""
            {"dr":{"gte":"1971-01-01 00:00:00.000","lte":"1972-01-01 00:00:00.000"}}""");
        refreshIndex();

        assertHitCount(
            client().prepareSearch(INDEX)
                .setQuery(rangeQuery("dr").gte("1969-07-10 00:00:00.000").lte("1969-07-25 00:00:00.000"))
                .setSize(0)
                .setTrackTotalHits(true),
            1L
        );
    }

    public void testRangeQueryFindsDocumentSpanningUnixEpoch() throws Exception {
        createDateRangeIndex();
        index("""
            {"dr":{"gte":"1969-12-31 12:00:00.000","lte":"1970-01-01 12:00:00.000"}}""");
        index("""
            {"dr":{"gte":"1969-06-01 00:00:00.000","lte":"1969-08-01 00:00:00.000"}}""");
        refreshIndex();

        assertHitCount(
            client().prepareSearch(INDEX)
                .setQuery(rangeQuery("dr").gte("1969-12-31 00:00:00.000").lte("1970-01-02 00:00:00.000"))
                .setSize(0)
                .setTrackTotalHits(true),
            1L
        );

        assertHitCount(
            client().prepareSearch(INDEX)
                .setQuery(rangeQuery("dr").gte("1970-01-01 00:00:00.000").lte("1970-01-01 23:59:59.999"))
                .setSize(0)
                .setTrackTotalHits(true),
            1L
        );
    }

    public void testRangeQueryWithEpochMillisAgainstSpanningDocument() throws Exception {
        createDateRangeIndex();
        index("""
            {"dr":{"gte":"1969-12-31 12:00:00.000","lte":"1970-01-01 12:00:00.000"}}""");
        refreshIndex();

        DateFormatter formatter = DateFormatter.forPattern(DATE_FORMAT);
        long queryLo = formatter.parseMillis("1969-12-31 18:00:00.000");
        long queryHi = formatter.parseMillis("1970-01-01 06:00:00.000");
        assertTrue("query window should cross the epoch", queryLo < 0 && queryHi > 0);

        assertHitCount(
            client().prepareSearch(INDEX).setQuery(rangeQuery("dr").gte(queryLo).lte(queryHi)).setSize(0).setTrackTotalHits(true),
            1L
        );
    }

    public void testTermQueryOnPreEpochInstant() throws Exception {
        createDateRangeIndex();
        index("""
            {"dr":{"gte":"1969-06-01 00:00:00.000","lte":"1969-08-01 00:00:00.000"}}""");
        refreshIndex();

        assertHitCount(
            client().prepareSearch(INDEX).setQuery(termQuery("dr", "1969-07-15 00:00:00.000")).setSize(0).setTrackTotalHits(true),
            1L
        );
    }

    private void createDateRangeIndex() throws IOException {
        assertAcked(
            indicesAdmin().prepareCreate(INDEX)
                .setSettings(Settings.builder().put("index.number_of_shards", 1))
                .setMapping(
                    jsonBuilder().startObject()
                        .startObject("properties")
                        .startObject("dr")
                        .field("type", "date_range")
                        .field("format", DATE_FORMAT)
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .get()
        );
    }

    private void index(String source) {
        prepareIndex(INDEX).setSource(source, XContentType.JSON).get();
    }

    private void refreshIndex() {
        indicesAdmin().prepareRefresh(INDEX).get();
    }
}
