/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enterprisesearch.action;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

public final class XSearchResponseBuilder {

    private XSearchResponseBuilder() {
        throw new AssertionError("Class not meant for instantiation");
    }

    public static void buildResponse(SearchResponse searchResponse, XContentBuilder builder) throws IOException {
        builder.startObject();
        final SearchHits hits = searchResponse.getHits();
        final XContentBuilder results = builder.startArray("results");
        for (SearchHit hit : hits.getHits()) {
            final XContentBuilder hitBuilder = results.startObject();
            for (Map.Entry<String, Object> sourceEntry : hit.getSourceAsMap().entrySet()) {
                hitBuilder.field(sourceEntry.getKey(), sourceEntry.getValue());
            }
            hitBuilder.endObject();
        }
        results.endArray();

        builder.endObject();
    }
}
