/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockRequest;
import org.elasticsearch.action.admin.indices.readonly.TransportAddIndexBlockAction;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

public class SearchWithIndexBlocksIT extends ESIntegTestCase {

    public void testSearchIndexWithIndexRefreshBlock() {
        createIndex("test");

        var addIndexBlockRequest = new AddIndexBlockRequest(IndexMetadata.APIBlock.REFRESH, "test");
        client().execute(TransportAddIndexBlockAction.TYPE, addIndexBlockRequest).actionGet();

        indexRandom(
            true,
            prepareIndex("test").setId("1").setSource("field", "value"),
            prepareIndex("test").setId("2").setSource("field", "value"),
            prepareIndex("test").setId("3").setSource("field", "value"),
            prepareIndex("test").setId("4").setSource("field", "value"),
            prepareIndex("test").setId("5").setSource("field", "value"),
            prepareIndex("test").setId("6").setSource("field", "value")
        );

        assertHitCount(prepareSearch().setQuery(QueryBuilders.matchAllQuery()), 0);
    }

    public void testSearchMultipleIndicesEachWithAnIndexRefreshBlock() {
        createIndex("test");
        createIndex("test2");

        var addIndexBlockRequest = new AddIndexBlockRequest(IndexMetadata.APIBlock.REFRESH, "test", "test2");
        client().execute(TransportAddIndexBlockAction.TYPE, addIndexBlockRequest).actionGet();

        indexRandom(
            true,
            prepareIndex("test").setId("1").setSource("field", "value"),
            prepareIndex("test").setId("2").setSource("field", "value"),
            prepareIndex("test").setId("3").setSource("field", "value"),
            prepareIndex("test").setId("4").setSource("field", "value"),
            prepareIndex("test").setId("5").setSource("field", "value"),
            prepareIndex("test").setId("6").setSource("field", "value"),
            prepareIndex("test2").setId("1").setSource("field", "value"),
            prepareIndex("test2").setId("2").setSource("field", "value"),
            prepareIndex("test2").setId("3").setSource("field", "value"),
            prepareIndex("test2").setId("4").setSource("field", "value"),
            prepareIndex("test2").setId("5").setSource("field", "value"),
            prepareIndex("test2").setId("6").setSource("field", "value")
        );

        assertHitCount(prepareSearch().setQuery(QueryBuilders.matchAllQuery()), 0);
    }

    public void testSearchMultipleIndicesWithOneIndexRefreshBlock() {
        createIndex("test");
        createIndex("test2");

        // Only block test
        var addIndexBlockRequest = new AddIndexBlockRequest(IndexMetadata.APIBlock.REFRESH, "test");
        client().execute(TransportAddIndexBlockAction.TYPE, addIndexBlockRequest).actionGet();

        indexRandom(
            true,
            prepareIndex("test").setId("1").setSource("field", "value"),
            prepareIndex("test").setId("2").setSource("field", "value"),
            prepareIndex("test").setId("3").setSource("field", "value"),
            prepareIndex("test").setId("4").setSource("field", "value"),
            prepareIndex("test").setId("5").setSource("field", "value"),
            prepareIndex("test").setId("6").setSource("field", "value"),
            prepareIndex("test2").setId("1").setSource("field", "value"),
            prepareIndex("test2").setId("2").setSource("field", "value"),
            prepareIndex("test2").setId("3").setSource("field", "value"),
            prepareIndex("test2").setId("4").setSource("field", "value"),
            prepareIndex("test2").setId("5").setSource("field", "value"),
            prepareIndex("test2").setId("6").setSource("field", "value")
        );

        // We should get test2 results (not blocked)
        assertHitCount(prepareSearch().setQuery(QueryBuilders.matchAllQuery()), 6);
    }

    public void testOpenPITWithIndexRefreshBlock() {
        createIndex("test");

        var addIndexBlockRequest = new AddIndexBlockRequest(IndexMetadata.APIBlock.REFRESH, "test");
        client().execute(TransportAddIndexBlockAction.TYPE, addIndexBlockRequest).actionGet();

        indexRandom(
            true,
            prepareIndex("test").setId("1").setSource("field", "value"),
            prepareIndex("test").setId("2").setSource("field", "value"),
            prepareIndex("test").setId("3").setSource("field", "value"),
            prepareIndex("test").setId("4").setSource("field", "value"),
            prepareIndex("test").setId("5").setSource("field", "value"),
            prepareIndex("test").setId("6").setSource("field", "value")
        );

        BytesReference pitId = null;
        try {
            OpenPointInTimeRequest openPITRequest = new OpenPointInTimeRequest("test").keepAlive(TimeValue.timeValueSeconds(10))
                .allowPartialSearchResults(true);
            pitId = client().execute(TransportOpenPointInTimeAction.TYPE, openPITRequest).actionGet().getPointInTimeId();
            SearchRequest searchRequest = new SearchRequest().source(
                new SearchSourceBuilder().pointInTimeBuilder(new PointInTimeBuilder(pitId).setKeepAlive(TimeValue.timeValueSeconds(10)))
            );
            SearchResponse searchResponse = client().search(searchRequest).actionGet();
            assertHitCount(searchResponse, 0);
        } finally {
            if (pitId != null) {
                client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pitId)).actionGet();
            }
        }
    }
}
