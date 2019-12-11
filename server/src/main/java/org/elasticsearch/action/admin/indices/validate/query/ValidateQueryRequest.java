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

package org.elasticsearch.action.admin.indices.validate.query;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;
import java.util.Arrays;

/**
 * A request to validate a specific query.
 * <p>
 * The request requires the query to be set using {@link #query(QueryBuilder)}
 */
public class ValidateQueryRequest extends BroadcastRequest<ValidateQueryRequest> implements ToXContentObject {

    private QueryBuilder query = new MatchAllQueryBuilder();

    private boolean explain;
    private boolean rewrite;
    private boolean allShards;

    long nowInMillis;

    public ValidateQueryRequest() {
        this(Strings.EMPTY_ARRAY);
    }

    public ValidateQueryRequest(StreamInput in) throws IOException {
        super(in);
        query = in.readNamedWriteable(QueryBuilder.class);
        if (in.getVersion().before(Version.V_8_0_0)) {
            int typesSize = in.readVInt();
            if (typesSize > 0) {
                for (int i = 0; i < typesSize; i++) {
                    in.readString();
                }
            }
        }
        explain = in.readBoolean();
        rewrite = in.readBoolean();
        allShards = in.readBoolean();
    }

    /**
     * Constructs a new validate request against the provided indices. No indices provided means it will
     * run against all indices.
     */
    public ValidateQueryRequest(String... indices) {
        super(indices);
        indicesOptions(IndicesOptions.fromOptions(false, false, true, false));
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (query == null) {
            validationException = ValidateActions.addValidationError("query cannot be null", validationException);
        }
        return validationException;
    }

    /**
     * The query to validate.
     */
    public QueryBuilder query() {
        return query;
    }

    public ValidateQueryRequest query(QueryBuilder query) {
        this.query = query;
        return this;
    }

    /**
     * Indicate if detailed information about query is requested
     */
    public void explain(boolean explain) {
        this.explain = explain;
    }

    /**
     * Indicates if detailed information about query is requested
     */
    public boolean explain() {
        return explain;
    }

    /**
     * Indicates whether the query should be rewritten into primitive queries
     */
    public void rewrite(boolean rewrite) {
        this.rewrite = rewrite;
    }

    /**
     * Indicates whether the query should be rewritten into primitive queries
     */
    public boolean rewrite() {
        return rewrite;
    }

    /**
     * Indicates whether the query should be validated on all shards instead of one random shard
     */
    public void allShards(boolean allShards) {
        this.allShards = allShards;
    }

    /**
     * Indicates whether the query should be validated on all shards instead of one random shard
     */
    public boolean allShards() {
        return allShards;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeNamedWriteable(query);
        if (out.getVersion().before(Version.V_8_0_0)) {
            out.writeVInt(0);   // no types to filter
        }
        out.writeBoolean(explain);
        out.writeBoolean(rewrite);
        out.writeBoolean(allShards);
    }

    @Override
    public String toString() {
        return "[" + Arrays.toString(indices) + "] query[" + query + "], explain:" + explain +
                ", rewrite:" + rewrite + ", all_shards:" + allShards;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("query");
        query.toXContent(builder, params);
        return builder.endObject();
    }
}
