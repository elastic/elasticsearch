/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.search;

import org.apache.lucene.index.IndexReader;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A query that wraps multiple sub-queries generated from a graph token stream.
 */
public final class GraphQuery extends Query {
    private final Query[] queries;
    private final boolean hasBoolean;

    /**
     * Constructor sets the queries and checks if any of them are
     * a boolean query.
     *
     * @param queries the non-null array of queries
     */
    public GraphQuery(Query... queries) {
        this.queries = Objects.requireNonNull(queries).clone();
        for (Query query : queries) {
            if (query instanceof BooleanQuery) {
                hasBoolean = true;
                return;
            }
        }
        hasBoolean = false;
    }

    /**
     * Gets the queries
     *
     * @return unmodifiable list of Query
     */
    public List<Query> getQueries() {
        return Collections.unmodifiableList(Arrays.asList(queries));
    }

    /**
     * If there is at least one boolean query or not.
     *
     * @return true if there is a boolean, false if not
     */
    public boolean hasBoolean() {
        return hasBoolean;
    }

    /**
     * Rewrites to a single query or a boolean query where each query is a SHOULD clause.
     */
    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        if (queries.length == 0) {
            return new BooleanQuery.Builder().build();
        }

        if (queries.length == 1) {
            return queries[0];
        }

        BooleanQuery.Builder q = new BooleanQuery.Builder();
        q.setDisableCoord(true);
        for (Query clause : queries) {
            q.add(clause, BooleanClause.Occur.SHOULD);
        }

        return q.build();
    }

    @Override
    public String toString(String field) {
        StringBuilder builder = new StringBuilder("Graph(");
        for (int i = 0; i < queries.length; i++) {
            if (i != 0) {
                builder.append(", ");
            }
            builder.append(Objects.toString(queries[i]));
        }
        builder.append(")");
        return builder.toString();
    }

    @Override
    public boolean equals(Object other) {
        return sameClassAs(other) &&
            Arrays.equals(queries, ((GraphQuery) other).queries);
    }

    @Override
    public int hashCode() {
        return 31 * classHash() + Arrays.hashCode(queries);
    }
}
