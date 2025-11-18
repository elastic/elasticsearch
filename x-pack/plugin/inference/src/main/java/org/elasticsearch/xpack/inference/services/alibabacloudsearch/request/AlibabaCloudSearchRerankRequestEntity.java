/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.request;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.rerank.AlibabaCloudSearchRerankTaskSettings;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public record AlibabaCloudSearchRerankRequestEntity(
    String query,
    List<String> input,
    @Nullable Boolean returnDocuments,
    @Nullable Integer topN,
    AlibabaCloudSearchRerankTaskSettings taskSettings
) implements ToXContentObject {

    private static final String SEARCH_QUERY = "query";
    private static final String TEXTS_FIELD = "docs";

    public AlibabaCloudSearchRerankRequestEntity {
        Objects.requireNonNull(query);
        Objects.requireNonNull(input);
        Objects.requireNonNull(taskSettings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(SEARCH_QUERY, query);
            builder.field(TEXTS_FIELD, input);
        }
        builder.endObject();
        return builder;
    }

}
