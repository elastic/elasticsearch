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
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;
import java.util.Arrays;

/**
 * A request to validate a specific query.
 * <p>
 * The request requires the query to be set using {@link #query(QueryBuilder)}
 */
public class ValidateQueryRequest extends BroadcastRequest<ValidateQueryRequest> {

    private QueryBuilder query = new MatchAllQueryBuilder();

    private boolean explain;
    private boolean rewrite;
    private boolean allShards;

    private String[] types = Strings.EMPTY_ARRAY;

    long nowInMillis;

    public ValidateQueryRequest() {
        this(Strings.EMPTY_ARRAY);
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
     * The types of documents the query will run against. Defaults to all types.
     */
    public String[] types() {
        return this.types;
    }

    /**
     * The types of documents the query will run against. Defaults to all types.
     */
    public ValidateQueryRequest types(String... types) {
        this.types = types;
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
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        query = in.readNamedWriteable(QueryBuilder.class);
        int typesSize = in.readVInt();
        if (typesSize > 0) {
            types = new String[typesSize];
            for (int i = 0; i < typesSize; i++) {
                types[i] = in.readString();
            }
        }
        explain = in.readBoolean();
        rewrite = in.readBoolean();
        if (in.getVersion().onOrAfter(Version.V_5_4_0)) {
            allShards = in.readBoolean();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeNamedWriteable(query);
        out.writeVInt(types.length);
        for (String type : types) {
            out.writeString(type);
        }
        out.writeBoolean(explain);
        out.writeBoolean(rewrite);
        if (out.getVersion().onOrAfter(Version.V_5_4_0)) {
            out.writeBoolean(allShards);
        }
    }

    @Override
    public String toString() {
        return "[" + Arrays.toString(indices) + "]" + Arrays.toString(types) + ", query[" + query + "], explain:" + explain +
                ", rewrite:" + rewrite + ", all_shards:" + allShards;
    }
}
