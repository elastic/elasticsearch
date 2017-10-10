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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Span query that matches the union of its clauses. Maps to {@link SpanOrQuery}.
 */
public class SpanOrQueryBuilder extends AbstractQueryBuilder<SpanOrQueryBuilder> implements SpanQueryBuilder {
    public static final String NAME = "span_or";

    private static final ParseField CLAUSES_FIELD = new ParseField("clauses");

    private final List<SpanQueryBuilder> clauses = new ArrayList<>();

    public SpanOrQueryBuilder(SpanQueryBuilder initialClause) {
        if (initialClause == null) {
            throw new IllegalArgumentException("[" + NAME + "] must include at least one clause");
        }
        clauses.add(initialClause);
    }

    /**
     * Read from a stream.
     */
    public SpanOrQueryBuilder(StreamInput in) throws IOException {
        super(in);
        for (QueryBuilder clause: readQueries(in)) {
            clauses.add((SpanQueryBuilder) clause);
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        writeQueries(out, clauses);
    }

    /**
     * Add a span clause to the current list of clauses
     */
    public SpanOrQueryBuilder addClause(SpanQueryBuilder clause) {
        if (clause == null) {
            throw new IllegalArgumentException("[" + NAME + "] inner clause cannot be null");
        }
        clauses.add(clause);
        return this;
    }

    /**
     * @return the {@link SpanQueryBuilder} clauses that were set for this query
     */
    public List<SpanQueryBuilder> clauses() {
        return Collections.unmodifiableList(this.clauses);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startArray(CLAUSES_FIELD.getPreferredName());
        for (SpanQueryBuilder clause : clauses) {
            clause.toXContent(builder, params);
        }
        builder.endArray();
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public static SpanOrQueryBuilder fromXContent(XContentParser parser) throws IOException {
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;

        List<SpanQueryBuilder> clauses = new ArrayList<>();

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (CLAUSES_FIELD.match(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        QueryBuilder query = parseInnerQueryBuilder(parser);
                        if (query instanceof SpanQueryBuilder == false) {
                            throw new ParsingException(parser.getTokenLocation(), "spanOr [clauses] must be of type span query");
                        }
                        clauses.add((SpanQueryBuilder) query);
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[span_or] query does not support [" + currentFieldName + "]");
                }
            } else {
                if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName)) {
                    boost = parser.floatValue();
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName)) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[span_or] query does not support [" + currentFieldName + "]");
                }
            }
        }

        if (clauses.isEmpty()) {
            throw new ParsingException(parser.getTokenLocation(), "spanOr must include [clauses]");
        }

        SpanOrQueryBuilder queryBuilder = new SpanOrQueryBuilder(clauses.get(0));
        for (int i = 1; i < clauses.size(); i++) {
            queryBuilder.addClause(clauses.get(i));
        }
        queryBuilder.boost(boost);
        queryBuilder.queryName(queryName);
        return queryBuilder;
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
