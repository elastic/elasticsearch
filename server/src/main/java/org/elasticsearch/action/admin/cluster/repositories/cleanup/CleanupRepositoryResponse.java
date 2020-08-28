/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.action.admin.cluster.repositories.cleanup;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.repositories.RepositoryCleanupResult;

import java.io.IOException;

public final class CleanupRepositoryResponse extends ActionResponse implements ToXContentObject {

    private static final ObjectParser<CleanupRepositoryResponse, Void> PARSER =
        new ObjectParser<>(CleanupRepositoryResponse.class.getName(), true, CleanupRepositoryResponse::new);

    static {
        PARSER.declareObject((response, cleanupResult) -> response.result = cleanupResult,
            RepositoryCleanupResult.PARSER, new ParseField("results"));
    }

    private RepositoryCleanupResult result;

    public CleanupRepositoryResponse() {
    }

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
