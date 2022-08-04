/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enterprisesearch.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

public class EntSearchResponse extends ActionResponse implements StatusToXContentObject {

    private SearchResponse searchResponse;

    public EntSearchResponse() {
    }

    public EntSearchResponse(StreamInput in) throws IOException {
        super(in);
        searchResponse = in.readOptionalWriteable(SearchResponse::new);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
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

        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(searchResponse);
    }

    @Override
    public RestStatus status() {
        if (hasResponse()) {
            return searchResponse.status();
        } else {
            return RestStatus.OK;
        }
    }

    public boolean hasResponse() {
        return searchResponse != null;
    }

    public SearchResponse getSearchResponse() {
        return searchResponse;
    }

    public void setSearchResponse(SearchResponse searchResponse) {
        this.searchResponse = searchResponse;
    }
}
