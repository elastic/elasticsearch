/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.enrich.LocalStateEnrich;

import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class EnrichShardMultiSearchActionTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LocalStateEnrich.class);
    }

    public void testExecute() throws Exception {
        XContentBuilder source = XContentBuilder.builder(XContentType.SMILE.xContent());
        source.startObject();
        source.field("key1", "value1");
        source.field("key2", "value2");
        source.endObject();

        String indexName = EnrichPolicy.ENRICH_INDEX_NAME_BASE + "1";
        IndexRequest indexRequest = new IndexRequest(indexName);
        indexRequest.source(source);
        client().index(indexRequest).actionGet();
        client().admin().indices().refresh(new RefreshRequest(indexName)).actionGet();

        int numSearches = randomIntBetween(2, 32);
        MultiSearchRequest request = new MultiSearchRequest();
        for (int i = 0; i < numSearches; i++) {
            SearchRequest searchRequest = new SearchRequest(indexName);
            searchRequest.source().from(0);
            searchRequest.source().size(1);
            searchRequest.source().query(new MatchAllQueryBuilder());
            searchRequest.source().fetchSource("key1", null);
            request.add(searchRequest);
        }

        MultiSearchResponse result = client().execute(
            EnrichShardMultiSearchAction.INSTANCE,
            new EnrichShardMultiSearchAction.Request(request)
        ).actionGet();
        assertThat(result.getResponses().length, equalTo(numSearches));
        for (int i = 0; i < numSearches; i++) {
            assertThat(result.getResponses()[i].isFailure(), is(false));
            assertThat(result.getResponses()[i].getResponse().getHits().getTotalHits().value, equalTo(1L));
            assertThat(result.getResponses()[i].getResponse().getHits().getHits()[0].getSourceAsMap().size(), equalTo(1));
            assertThat(result.getResponses()[i].getResponse().getHits().getHits()[0].getSourceAsMap().get("key1"), equalTo("value1"));
        }
    }

    public void testNonEnrichIndex() throws Exception {
        createIndex("index");
        MultiSearchRequest request = new MultiSearchRequest();
        request.add(new SearchRequest("index"));
        Exception e = expectThrows(
            ActionRequestValidationException.class,
            () -> client().execute(EnrichShardMultiSearchAction.INSTANCE, new EnrichShardMultiSearchAction.Request(request)).actionGet()
        );
        assertThat(e.getMessage(), equalTo("Validation Failed: 1: index [index] is not an enrich index;"));
    }

    public void testMultipleShards() throws Exception {
        String indexName = EnrichPolicy.ENRICH_INDEX_NAME_BASE + "1";
        createIndex(indexName, Settings.builder().put("index.number_of_shards", 2).build());
        MultiSearchRequest request = new MultiSearchRequest();
        request.add(new SearchRequest(indexName));
        Exception e = expectThrows(
            IllegalStateException.class,
            () -> client().execute(EnrichShardMultiSearchAction.INSTANCE, new EnrichShardMultiSearchAction.Request(request)).actionGet()
        );
        assertThat(e.getMessage(), equalTo("index [.enrich-1] should have 1 shard, but has 2 shards"));
    }

    public void testMultipleIndices() throws Exception {
        MultiSearchRequest request = new MultiSearchRequest();
        request.add(new SearchRequest("index1"));
        request.add(new SearchRequest("index2"));
        expectThrows(AssertionError.class, () -> new EnrichShardMultiSearchAction.Request(request));
    }

}
