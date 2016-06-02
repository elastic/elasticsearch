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
package org.elasticsearch.index.query.functionscore;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.function.QueryFunction;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.Objects;

/**
 * A function that computes a score according to the provided query.
 */
public class QueryFunctionBuilder extends ScoreFunctionBuilder<QueryFunctionBuilder> {
    public static final String NAME = "query_score";
    public static final ParseField FUNCTION_NAME_FIELD = new ParseField(NAME);
    QueryBuilder queryBuilder;

    public QueryFunctionBuilder(StreamInput in) throws IOException {
        super(in);
        queryBuilder = in.readNamedWriteable(QueryBuilder.class);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(queryBuilder);
    }

    @Override
    public String getName() {
        return NAME;
    }

    public QueryFunctionBuilder(QueryBuilder query) {
        this.queryBuilder = query;
        if (queryBuilder == null) {
            throw new IllegalArgumentException("Query in query_score must not be null.");
        }
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(getName());
        queryBuilder.toXContent(builder, params);
    }

    @Override
    protected boolean doEquals(QueryFunctionBuilder functionBuilder) {
        return Objects.equals(this.queryBuilder, functionBuilder.queryBuilder);
    }

    @Override
    public int doHashCode() {
        return Objects.hash(this.queryBuilder);
    }

    @Override
    protected ScoreFunction doToFunction(QueryShardContext context) throws IOException {
        Query query = queryBuilder.toQuery(context);
        return new QueryFunction(query);
    }

    public static QueryFunctionBuilder fromXContent(QueryParseContext parseContext)
        throws IOException, ParsingException {
        XContentParser parser = parseContext.parser();
        XContentParser.Token token = parser.currentToken();
        if (token.compareTo(XContentParser.Token.START_OBJECT) != 0) {
            throw new ParsingException(parser.getTokenLocation(), "Expected START OBJECT");
        }
        QueryBuilder queryBuilder = parseContext.parseInnerQueryBuilder();
        if (parser.currentToken().compareTo(XContentParser.Token.END_OBJECT) != 0) {
            throw new ParsingException(parser.getTokenLocation(), "Expected } but got " + parser.currentToken());
        }
        return new QueryFunctionBuilder(queryBuilder);
    }
}
