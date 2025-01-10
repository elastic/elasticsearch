/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.stats;

import org.elasticsearch.action.admin.indices.stats.FieldUsageShardResponse;
import org.elasticsearch.action.admin.indices.stats.FieldUsageStatsAction;
import org.elasticsearch.action.admin.indices.stats.FieldUsageStatsRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.search.stats.FieldUsageStats;
import org.elasticsearch.index.search.stats.FieldUsageStats.UsageContext;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.test.ESIntegTestCase;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAllSuccessful;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;

public class FieldUsageStatsIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put("search.aggs.rewrite_to_filter_by_filter", false)
            .build();
    }

    private FieldUsageStats aggregated(List<FieldUsageShardResponse> stats) {
        assertFalse(stats.isEmpty());
        return stats.stream().map(FieldUsageShardResponse::getStats).reduce(FieldUsageStats::add).get();
    }

    public void testFieldUsageStats() throws ExecutionException, InterruptedException {
        internalCluster().ensureAtLeastNumDataNodes(2);
        int numShards = randomIntBetween(1, 2);
        assertAcked(indicesAdmin().prepareCreate("test").setSettings(indexSettings(numShards, 1)));

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd", Locale.ROOT);
        LocalDate date = LocalDate.of(2015, 9, 1);

        for (int i = 0; i < 30; i++) {
            prepareIndex("test").setId(Integer.toString(i))
                .setSource("field", "value", "field2", "value2", "date_field", formatter.format(date.plusDays(i)))
                .get();
        }
        indicesAdmin().prepareRefresh("test").get();

        ensureGreen("test");

        FieldUsageStats stats = aggregated(
            client().execute(FieldUsageStatsAction.INSTANCE, new FieldUsageStatsRequest()).get().getStats().get("test")
        );

        assertFalse(stats.hasField("field"));
        assertFalse(stats.hasField("field.keyword"));
        assertFalse(stats.hasField("field2"));
        assertFalse(stats.hasField("date_field"));

        assertResponse(
            prepareSearch().setSearchType(SearchType.DEFAULT)
                .setQuery(QueryBuilders.termQuery("field", "value"))
                .addAggregation(AggregationBuilders.terms("agg1").field("field.keyword"))
                .addAggregation(AggregationBuilders.filter("agg2", QueryBuilders.spanTermQuery("field2", "value2")))
                .setSize(between(5, 100))
                .setPreference("fixed"),
            response -> {
                assertHitCount(response, 30);
                assertAllSuccessful(response);
            }
        );

        stats = aggregated(client().execute(FieldUsageStatsAction.INSTANCE, new FieldUsageStatsRequest()).get().getStats().get("test"));
        logger.info("Stats after first query: {}", stats);

        assertTrue(stats.hasField("_id"));
        assertEquals(Set.of(UsageContext.STORED_FIELDS), stats.get("_id").keySet());
        assertTrue(stats.hasField("_source"));
        assertEquals(Set.of(UsageContext.STORED_FIELDS), stats.get("_source").keySet());

        assertTrue(stats.hasField("field"));
        // we sort by _score
        assertEquals(
            Set.of(UsageContext.TERMS, UsageContext.POSTINGS, UsageContext.FREQS, UsageContext.NORMS),
            stats.get("field").keySet()
        );
        assertEquals(1L * numShards, stats.get("field").getTerms());

        assertTrue(stats.hasField("field2"));
        // positions because of span query
        assertEquals(
            Set.of(UsageContext.TERMS, UsageContext.POSTINGS, UsageContext.FREQS, UsageContext.POSITIONS, UsageContext.NORMS),
            stats.get("field2").keySet()
        );
        assertEquals(1L * numShards, stats.get("field2").getTerms());

        assertTrue(stats.hasField("field.keyword"));
        // terms agg does not use search as we've set search.aggs.rewrite_to_filter_by_filter to false
        assertEquals(Set.of(UsageContext.DOC_VALUES), stats.get("field.keyword").keySet());
        assertEquals(1L * numShards, stats.get("field.keyword").getDocValues());

        prepareSearch().setSearchType(SearchType.DEFAULT)
            .setQuery(QueryBuilders.termQuery("field", "value"))
            .addAggregation(AggregationBuilders.terms("agg1").field("field.keyword"))
            .setSize(0)
            .setPreference("fixed")
            .get()
            .decRef();

        stats = aggregated(client().execute(FieldUsageStatsAction.INSTANCE, new FieldUsageStatsRequest()).get().getStats().get("test"));
        logger.info("Stats after second query: {}", stats);

        assertEquals(2L * numShards, stats.get("field").getTerms());
        assertEquals(1L * numShards, stats.get("field2").getTerms());
        assertEquals(2L * numShards, stats.get("field.keyword").getDocValues());

        assertFalse(stats.hasField("date_field"));

        // show that we also track stats in can_match
        assertEquals(
            2L * numShards,
            indicesAdmin().prepareStats("test")
                .clear()
                .setSearch(true)
                .get()
                .getIndex("test")
                .getTotal()
                .getSearch()
                .getTotal()
                .getQueryCount()
        );
        prepareSearch().setSearchType(SearchType.DEFAULT)
            .setPreFilterShardSize(1)
            .setQuery(QueryBuilders.rangeQuery("date_field").from("2016/01/01"))
            .setSize(100)
            .setPreference("fixed")
            .get()
            .decRef();

        stats = aggregated(client().execute(FieldUsageStatsAction.INSTANCE, new FieldUsageStatsRequest()).get().getStats().get("test"));
        logger.info("Stats after third query: {}", stats);

        assertTrue(stats.hasField("date_field"));
        assertEquals(Set.of(UsageContext.POINTS), stats.get("date_field").keySet());

        long expectedShards = 2L * numShards;
        if (numShards == 1) {
            // with 1 shard and setPreFilterShardSize(1) we don't perform can_match phase but instead directly query the shard
            expectedShards += 1;
        }

        assertEquals(
            expectedShards,
            indicesAdmin().prepareStats("test")
                .clear()
                .setSearch(true)
                .get()
                .getIndex("test")
                .getTotal()
                .getSearch()
                .getTotal()
                .getQueryCount()
        );
    }
}
