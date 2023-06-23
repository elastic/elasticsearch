/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.search.action;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

public class SearchApplicationSearchResponse extends SearchResponse {

    public Map<String,String> indexWarnings;

    public SearchApplicationSearchResponse(SearchResponse searchResponse, @Nullable Map<String,String> indexWarnings) {
        super(searchResponse);
        this.indexWarnings = indexWarnings;
    }

    public Map<String,String> indexWarnings() { return indexWarnings; }

    @Override
    public XContentBuilder headerToXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        super.headerToXContent(builder, params);
        builder.field("index_warnings", indexWarnings);
        return builder;
    }
}
