/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.cluster.repositories.cleanup;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.repositories.RepositoryCleanupResult;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public final class CleanupRepositoryResponse extends ActionResponse implements ToXContentObject {

    private static final ObjectParser<CleanupRepositoryResponse, Void> PARSER = new ObjectParser<>(
        CleanupRepositoryResponse.class.getName(),
        true,
        CleanupRepositoryResponse::new
    );

    static {
        PARSER.declareObject(
            (response, cleanupResult) -> response.result = cleanupResult,
            RepositoryCleanupResult.PARSER,
            new ParseField("results")
        );
    }

    private RepositoryCleanupResult result;

    public CleanupRepositoryResponse() {}

    public CleanupRepositoryResponse(RepositoryCleanupResult result) {
        this.result = result;
    }

    public CleanupRepositoryResponse(StreamInput in) throws IOException {
        result = new RepositoryCleanupResult(in);
    }

    public RepositoryCleanupResult result() {
        return result;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        result.writeTo(out);
    }

    public static CleanupRepositoryResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject().field("results");
        result.toXContent(builder, params);
        builder.endObject();
        return builder;
    }
}
