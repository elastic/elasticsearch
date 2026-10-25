/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.reindex.BulkByPaginatedSearchResponse;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.xpack.esql.VerificationException;

import java.util.Collection;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class EsqlReindexIT extends AbstractEsqlIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), ReindexPlugin.class);
    }

    public void testReindexCopiesEveryRowOfTheQuery() {
        // More rows than the default ES|QL result limit and more than one streamed batch.
        int docs = between(2_001, 3_000);
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < docs; i++) {
            bulk.add(prepareIndex("source").setSource("id", i, "level", i % 2 == 0 ? "error" : "info"));
        }
        assertNoFailures(bulk.get());

        ReindexRequest request = new ReindexRequest().setEsqlQuery("FROM source | WHERE level == \"error\" | EVAL copied = id + 1")
            .setDestIndex("dest");
        BulkByPaginatedSearchResponse response = client().execute(ReindexAction.INSTANCE, request).actionGet();

        long expected = (docs + 1) / 2;
        assertThat(response.getBulkFailures(), empty());
        assertThat(response.getCreated(), equalTo(expected));
        assertThat(response.getTotal(), equalTo(expected));
        refresh("dest");
        assertHitCount(prepareSearch("dest").setSize(0).setTrackTotalHits(true), expected);
        assertResponse(prepareSearch("dest").setQuery(termQuery("id", 10)), search -> {
            Map<String, Object> source = search.getHits().getAt(0).getSourceAsMap();
            assertThat(source, equalTo(Map.of("id", 10, "level", "error", "copied", 11)));
        });
    }

    public void testReindexReportsQueryErrors() {
        ReindexRequest request = new ReindexRequest().setEsqlQuery("FROM missing_index").setDestIndex("dest");
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> client().execute(ReindexAction.INSTANCE, request).actionGet()
        );
        assertThat(e.getMessage(), containsString("Unknown index [missing_index]"));
        assertFalse(indexExists("dest"));
    }
}
