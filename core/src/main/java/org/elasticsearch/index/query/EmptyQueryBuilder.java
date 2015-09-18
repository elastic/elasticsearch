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

package org.elasticsearch.index.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

/**
 * A {@link QueryBuilder} that is a stand in replacement for an empty query clause in the DSL.
 * The current DSL allows parsing inner queries / filters like "{ }", in order to have a
 * valid non-null representation of these clauses that actually do nothing we can use this class.
 *
 * This builder has no corresponding parser and it is not registered under the query name. It is
 * intended to be used internally as a stand-in for nested queries that are left empty and should
 * be ignored upstream.
 */
public class EmptyQueryBuilder extends ToXContentToBytes implements QueryBuilder<EmptyQueryBuilder> {

    public static final String NAME = "empty_query";

    /** the one and only empty query builder */
    public static final EmptyQueryBuilder PROTOTYPE = new EmptyQueryBuilder();

    // prevent instances other than prototype
    private EmptyQueryBuilder() {
        super(XContentType.JSON);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public String getName() {
        return getWriteableName();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.endObject();
        return builder;
    }

    @Override
    public Query toQuery(QueryShardContext context) throws IOException {
        // empty
        return null;
    }

    @Override
    public Query toFilter(QueryShardContext context) throws IOException {
        // empty
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
    }

    @Override
    public EmptyQueryBuilder readFrom(StreamInput in) throws IOException {
        return EmptyQueryBuilder.PROTOTYPE;
    }

    @Override
    public EmptyQueryBuilder queryName(String queryName) {
        //no-op
        return this;
    }

    @Override
    public String queryName() {
        return null;
    }

    @Override
    public float boost() {
        return -1;
    }

    @Override
    public EmptyQueryBuilder boost(float boost) {
        //no-op
        return this;
    }
}
