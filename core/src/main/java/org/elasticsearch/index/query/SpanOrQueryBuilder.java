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
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Span query that matches the union of its clauses. Maps to {@link SpanOrQuery}.
 */
public class SpanOrQueryBuilder extends AbstractQueryBuilder<SpanOrQueryBuilder> implements SpanQueryBuilder<SpanOrQueryBuilder> {

    public static final String NAME = "span_or";

    private final List<SpanQueryBuilder> clauses = new ArrayList<>();

    static final SpanOrQueryBuilder PROTOTYPE = new SpanOrQueryBuilder(SpanTermQueryBuilder.PROTOTYPE);

    public SpanOrQueryBuilder(SpanQueryBuilder initialClause) {
        if (initialClause == null) {
            throw new IllegalArgumentException("query must include at least one clause");
        }
        clauses.add(initialClause);
    }

    public SpanOrQueryBuilder clause(SpanQueryBuilder clause) {
        if (clause == null) {
            throw new IllegalArgumentException("inner bool query clause cannot be null");
        }
        clauses.add(clause);
        return this;
    }

    /**
     * @return the {@link SpanQueryBuilder} clauses that were set for this query
     */
    public List<SpanQueryBuilder> clauses() {
        return this.clauses;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startArray(SpanOrQueryParser.CLAUSES_FIELD.getPreferredName());
        for (SpanQueryBuilder clause : clauses) {
            clause.toXContent(builder, params);
        }
        builder.endArray();
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        SpanQuery[] spanQueries = new SpanQuery[clauses.size()];
        for (int i = 0; i < clauses.size(); i++) {
            Query query = clauses.get(i).toQuery(context);
            assert query instanceof SpanQuery;
            spanQueries[i] = (SpanQuery) query;
        }
        return new SpanOrQuery(spanQueries);
    }

    @Override
    protected SpanOrQueryBuilder doReadFrom(StreamInput in) throws IOException {
        List<QueryBuilder> clauses = readQueries(in);
        SpanOrQueryBuilder queryBuilder = new SpanOrQueryBuilder((SpanQueryBuilder)clauses.get(0));
        for (int i = 1; i < clauses.size(); i++) {
            queryBuilder.clauses.add((SpanQueryBuilder)clauses.get(i));
        }
        return queryBuilder;

    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        writeQueries(out, clauses);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(clauses);
    }

    @Override
    protected boolean doEquals(SpanOrQueryBuilder other) {
        return Objects.equals(clauses, other.clauses);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
