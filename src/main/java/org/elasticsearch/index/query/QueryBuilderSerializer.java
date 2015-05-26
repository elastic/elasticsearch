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

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;

import java.io.IOException;

/**
 * Reads and writes a {@link QueryBuilder} when serialized over the wire based on its name which gets serialized
 * before the the query itself and allows, when reading, to create the proper {@link QueryBuilder} subclass instance.
 */
public class QueryBuilderSerializer {

    @Inject
    static IndicesQueriesRegistry indicesQueriesRegistry;

    /**
     * Reads a {@link QueryBuilder} serialized over the wire.
     * Depends on the {@link IndicesQueriesRegistry} being injected via guice, which happens only within nodes. The registry
     * will be null within a transport client but that is fine given that it will only need to write queries, never read them.
     */
    public static QueryBuilder read(StreamInput in) throws IOException {
        if (indicesQueriesRegistry == null) {
            throw new IllegalStateException("unable to read query, the queries registry is null");
        }
        String queryId = in.readString();
        QueryParser queryParser = indicesQueriesRegistry.queryParsers().get(queryId);
        if (queryParser == null) {
            throw new IllegalStateException("unable to read query, no query parser registered for [" + queryId + "]");
        }
        return queryParser.getBuilderPrototype().readFrom(in);
    }

    /**
     * Writes a {@link QueryBuilder} to be serialized over the wire
     */
    public static void write(QueryBuilder queryBuilder, StreamOutput out) throws IOException {
        out.writeString(queryBuilder.queryId());
        queryBuilder.writeTo(out);
    }
}
