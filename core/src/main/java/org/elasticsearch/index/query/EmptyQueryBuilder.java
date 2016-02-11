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
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Objects;

/**
 * A {@link QueryBuilder} that is a stand in replacement for an empty query clause in the DSL.
 * The current DSL allows parsing inner queries / filters like "{ }", in order to have a
 * valid non-null representation of these clauses that actually do nothing we can use this class.
 *
 * This builder has no corresponding parser and it is not registered under the query name. It is
 * intended to be used internally as a stand-in for nested queries that are left empty and should
 * be ignored upstream.
 */
public final class EmptyQueryBuilder extends AbstractQueryBuilder<EmptyQueryBuilder> {

    public static final String NAME = "empty_query";

    /** the one and only empty query builder */
    public static final EmptyQueryBuilder PROTOTYPE = new EmptyQueryBuilder();

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        return null;
    }

    @Override
    public String getName() {
        return getWriteableName();
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
    }


    @Override
    protected EmptyQueryBuilder doReadFrom(StreamInput in) throws IOException {
        return new EmptyQueryBuilder();
    }

    @Override
    protected int doHashCode() {
        return 31;
    }

    @Override
    protected boolean doEquals(EmptyQueryBuilder other) {
        return true;
    }
}
