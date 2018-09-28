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

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.common.lucene.search.function.ScriptScoreFunction;
import org.elasticsearch.common.lucene.search.function.ScriptScoreQuery;
import org.elasticsearch.index.query.InnerHitContextBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * A query that computes a document score based on the provided script
 */
public class ScriptScoreQueryBuilder extends AbstractQueryBuilder<ScriptScoreQueryBuilder> {
    public static final String NAME = "script_score";
    public static final ParseField QUERY_FIELD = new ParseField("query");
    public static final ParseField SCRIPT_FIELD = new ParseField("script");
    public static final ParseField MIN_SCORE_FIELD = new ParseField("min_score");

    private final QueryBuilder query;
    private Float minScore = null;
    private final ScriptScoreFunctionBuilder scriptScoreFunctionBuilder;


    /**
     * Creates a script_score query that executes the provided script function on documents that match a query.
     *
     * @param query the query that defines which documents the script_score query will be executed on.
     * @param scriptScoreFunctionBuilder defines script function
     */
    public ScriptScoreQueryBuilder(QueryBuilder query, ScriptScoreFunctionBuilder scriptScoreFunctionBuilder) {
        if (query == null) {
            throw new IllegalArgumentException("script_score: query must not be null");
        }
        if (scriptScoreFunctionBuilder == null) {
            throw new IllegalArgumentException("script_score: script must not be null");
        }
        this.query = query;
        this.scriptScoreFunctionBuilder = scriptScoreFunctionBuilder;
    }

    /**
     * Read from a stream.
     */
    public ScriptScoreQueryBuilder(StreamInput in) throws IOException {
        super(in);
        query = in.readNamedWriteable(QueryBuilder.class);
        scriptScoreFunctionBuilder = in.readNamedWriteable(ScriptScoreFunctionBuilder.class);
        minScore = in.readOptionalFloat();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(query);
        out.writeNamedWriteable(scriptScoreFunctionBuilder);
        out.writeOptionalFloat(minScore);
    }

    /**
     * Returns the query builder that defines which documents the script_score query will be executed on.
     */
    public QueryBuilder query() {
        return this.query;
    }

    /**
     * Returns the script function builder
     */
    public ScriptScoreFunctionBuilder scriptScoreFunctionBuilder() {
        return this.scriptScoreFunctionBuilder;
    }


    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        if (query != null) {
            builder.field(QUERY_FIELD.getPreferredName());
            query.toXContent(builder, params);
        }
        builder.field(SCRIPT_FIELD.getPreferredName(), scriptScoreFunctionBuilder.getScript());
        if (minScore != null) {
            builder.field(MIN_SCORE_FIELD.getPreferredName(), minScore);
        }
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public ScriptScoreQueryBuilder setMinScore(float minScore) {
        this.minScore = minScore;
        return this;
    }

    public Float getMinScore() {
        return this.minScore;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected boolean doEquals(ScriptScoreQueryBuilder other) {
        return Objects.equals(this.query, other.query) &&
            Objects.equals(this.scriptScoreFunctionBuilder, other.scriptScoreFunctionBuilder) &&
            Objects.equals(this.minScore, other.minScore) ;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(this.query, this.scriptScoreFunctionBuilder, this.minScore);
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        ScriptScoreFunction function = (ScriptScoreFunction) scriptScoreFunctionBuilder.toFunction(context);
        Query query = this.query.toQuery(context);
        if (query == null) {
            query = new MatchAllDocsQuery();
        }
        return new ScriptScoreQuery(query, function, minScore);
    }


    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        QueryBuilder newQuery = this.query.rewrite(queryRewriteContext);
        if (newQuery != query) {
            ScriptScoreQueryBuilder newQueryBuilder = new ScriptScoreQueryBuilder(newQuery, scriptScoreFunctionBuilder);
            newQueryBuilder.setMinScore(minScore);
            return newQueryBuilder;
        }
        return this;
    }

    @Override
    protected void extractInnerHitBuilders(Map<String, InnerHitContextBuilder> innerHits) {
        InnerHitContextBuilder.extractInnerHits(query(), innerHits);
    }

    public static ScriptScoreQueryBuilder fromXContent(XContentParser parser) throws IOException {
        QueryBuilder query = null;
        ScriptScoreFunctionBuilder functionBuilder = null;
        String queryName = null;
        Float minScore = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;

        String currentFieldName = null;
        XContentParser.Token token;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (QUERY_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    if (query != null) {
                        throw new ParsingException(
                            parser.getTokenLocation(), "failed to parse [{}] query. [query] is already defined.", NAME);
                    }
                    query = parseInnerQueryBuilder(parser);
                } else if (SCRIPT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    Script script = Script.parse(parser);
                    functionBuilder = new ScriptScoreFunctionBuilder(script);
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(), "failed to parse [{}] query. field [{}] is not supported", NAME, currentFieldName);
                }
            } else if (token.isValue()) {
                if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else if (MIN_SCORE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    minScore = parser.floatValue();
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(), "failed to parse [{}] query. field [{}] is not supported", NAME, currentFieldName);
                }
            } else {
                throw new ParsingException(
                    parser.getTokenLocation(), "failed to parse [{}] query. field [{}] is not supported", NAME, currentFieldName);
            }
        }

        if (query == null) {
            query = new MatchAllQueryBuilder();
        }
        ScriptScoreQueryBuilder scriptScoreQueryBuilder = new ScriptScoreQueryBuilder(query, functionBuilder);
        if (minScore != null) {
            scriptScoreQueryBuilder.setMinScore(minScore);
        }
        scriptScoreQueryBuilder.boost(boost);
        scriptScoreQueryBuilder.queryName(queryName);
        return scriptScoreQueryBuilder;
    }
}
