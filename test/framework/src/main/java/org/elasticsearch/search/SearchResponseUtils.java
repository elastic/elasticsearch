/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Response;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;

public enum SearchResponseUtils {
    ;

    public static TotalHits getTotalHits(SearchRequestBuilder request) {
        var resp = request.get();
        try {
            return resp.getHits().getTotalHits();
        } finally {
            resp.decRef();
        }
    }

    public static long getTotalHitsValue(SearchRequestBuilder request) {
        return getTotalHits(request).value;
    }

    public static SearchResponse responseAsSearchResponse(Response searchResponse) throws IOException {
        try (var parser = ESRestTestCase.responseAsParser(searchResponse)) {
            return SearchResponse.fromXContent(parser);
        }
    }

    public static SearchResponse emptyWithTotalHits(
        String scrollId,
        int totalShards,
        int successfulShards,
        int skippedShards,
        long tookInMillis,
        ShardSearchFailure[] shardFailures,
        SearchResponse.Clusters clusters
    ) {
        return new SearchResponse(
            SearchHits.EMPTY_WITH_TOTAL_HITS,
            null,
            null,
            false,
            null,
            null,
            1,
            scrollId,
            totalShards,
            successfulShards,
            skippedShards,
            tookInMillis,
            shardFailures,
            clusters
        );
    }
}
