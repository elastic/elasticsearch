/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.aggregations.bucket;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.aggregations.AggregationsPlugin;
import org.elasticsearch.aggregations.bucket.timeseries.TimeSeriesAggregationBuilder;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.metrics.ScriptedMetricAggregationBuilder;
import org.elasticsearch.test.AbstractSearchCancellationTestCase;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.index.IndexSettings.TIME_SERIES_END_TIME;
import static org.elasticsearch.index.IndexSettings.TIME_SERIES_START_TIME;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class SearchCancellationIT extends AbstractSearchCancellationTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(AggregationsPlugin.class);
        return List.copyOf(plugins);
    }

    public void testCancellationDuringTimeSeriesAggregation() throws Exception {
        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        int numberOfShards = between(2, 5);
        long now = Instant.now().toEpochMilli();
        int numberOfRefreshes = between(1, 5);
        // After a few initial checks we check every 2048 - number of shards records so we need to ensure all
        // shards have enough records to trigger a check
        int numberOfDocsPerRefresh = numberOfShards * between(3000, 3500) / numberOfRefreshes;
        assertAcked(
            prepareCreate("test").setSettings(
                indexSettings(numberOfShards, 0).put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.name())
                    .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "dim")
                    .put(TIME_SERIES_START_TIME.getKey(), now)
                    .put(TIME_SERIES_END_TIME.getKey(), now + (long) numberOfRefreshes * numberOfDocsPerRefresh + 1)
            ).setMapping("""
                {
                  "properties": {
                    "@timestamp": {"type": "date", "format": "epoch_millis"},
                    "dim": {"type": "keyword", "time_series_dimension": true}
                  }
                }
                """)
        );

        for (int i = 0; i < numberOfRefreshes; i++) {
            // Make sure we sometimes have a few segments
            BulkRequestBuilder bulkRequestBuilder = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int j = 0; j < numberOfDocsPerRefresh; j++) {
                bulkRequestBuilder.add(
                    prepareIndex("test").setOpType(DocWriteRequest.OpType.CREATE)
                        .setSource(
                            "@timestamp",
                            now + (long) i * numberOfDocsPerRefresh + j,
                            "val",
                            (double) j,
                            "dim",
                            String.valueOf(j % 100)
                        )
                );
            }
            assertNoFailures(bulkRequestBuilder.get());
        }

        logger.info("Executing search");
        Client client = client();
        TimeSeriesAggregationBuilder timeSeriesAggregationBuilder = new TimeSeriesAggregationBuilder("test_agg");
        ActionFuture<SearchResponse> searchResponse = client.prepareSearch("test")
            .setQuery(matchAllQuery())
            .addAggregation(
                timeSeriesAggregationBuilder.subAggregation(
                    new ScriptedMetricAggregationBuilder("sub_agg").initScript(
                        new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.INIT_SCRIPT_NAME, Collections.emptyMap())
                    )
                        .mapScript(
                            new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.MAP_BLOCK_SCRIPT_NAME, Collections.emptyMap())
                        )
                        .combineScript(
                            new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.COMBINE_SCRIPT_NAME, Collections.emptyMap())
                        )
                        .reduceScript(
                            new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.REDUCE_FAIL_SCRIPT_NAME, Collections.emptyMap())
                        )
                )
            )
            .execute();
        awaitForBlock(plugins);
        cancelSearch(TransportSearchAction.TYPE.name());
        disableBlocks(plugins);

        SearchPhaseExecutionException ex = expectThrows(SearchPhaseExecutionException.class, searchResponse::actionGet);
        assertThat(ExceptionsHelper.status(ex), equalTo(RestStatus.BAD_REQUEST));
        logger.info("All shards failed with", ex);
        if (lowLevelCancellation) {
            // Ensure that we cancelled in TimeSeriesIndexSearcher and not in reduce phase
            assertThat(ExceptionsHelper.stackTrace(ex), not(containsString("not building sub-aggregations due to task cancellation")));
        } else {
            assertThat(ExceptionsHelper.stackTrace(ex), containsString("not building sub-aggregations due to task cancellation"));
        }
    }
}
