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
import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.function.ScriptScoreQuery;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.InnerHitContextBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.Script;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * A query that computes a document score based on the provided script
 */
public class ScriptScoreQueryBuilder extends AbstractQueryBuilder<ScriptScoreQueryBuilder> {

    public static final String NAME = "script_score";
    public static final ParseField QUERY_FIELD = new ParseField("query");
    public static final ParseField SCRIPT_FIELD = new ParseField("script");
    public static final ParseField MIN_SCORE_FIELD = new ParseField("min_score");

    private static final ConstructingObjectParser<ScriptScoreQueryBuilder, Void> PARSER = new ConstructingObjectParser<>(NAME, false,
        args -> {
            ScriptScoreQueryBuilder ssQueryBuilder = new ScriptScoreQueryBuilder((QueryBuilder) args[0], (Script) args[1]);
            if (args[2] != null) ssQueryBuilder.setMinScore((Float) args[2]);
            if (args[3] != null) ssQueryBuilder.boost((Float) args[3]);
            if (args[4] != null) ssQueryBuilder.queryName((String) args[4]);
            return ssQueryBuilder;
        });

    static {
        PARSER.declareObject(constructorArg(), (p,c) -> parseInnerQueryBuilder(p), QUERY_FIELD);
        PARSER.declareObject(constructorArg(), (p,c) -> Script.parse(p), SCRIPT_FIELD);
        PARSER.declareFloat(optionalConstructorArg(), MIN_SCORE_FIELD);
        PARSER.declareFloat(optionalConstructorArg(), AbstractQueryBuilder.BOOST_FIELD);
        PARSER.declareString(optionalConstructorArg(), AbstractQueryBuilder.NAME_FIELD);
    }

    public static ScriptScoreQueryBuilder fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final QueryBuilder query;
    private Float minScore = null;
    private final Script script;


    /**
     * Creates a script_score query that executes the provided script function on documents that match a query.
     *
     * @param query the query that defines which documents the script_score query will be executed on.
     * @param script the script to run for computing the query score
     */
    public ScriptScoreQueryBuilder(QueryBuilder query, Script script) {
        // require the supply of the query, even the explicit supply of "match_all" query
        if (query == null) {
            throw new IllegalArgumentException("script_score: query must not be null");
        }
        if (script == null) {
            throw new IllegalArgumentException("script_score: script must not be null");
        }
        this.query = query;
        this.script = script;
    }

    /**
     * Read from a stream.
     */
    public ScriptScoreQueryBuilder(StreamInput in) throws IOException {
        super(in);
        query = in.readNamedWriteable(QueryBuilder.class);
        if (in.getVersion().onOrAfter(Version.V_7_5_0)) {
            script = new Script(in);
        } else {
            script = in.readNamedWriteable(ScriptScoreFunctionBuilder.class).getScript();
        }
        minScore = in.readOptionalFloat();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(query);
        if (out.getVersion().onOrAfter(Version.V_7_5_0)) {
            script.writeTo(out);
        } else {
            out.writeNamedWriteable(new ScriptScoreFunctionBuilder(script));
        }
        out.writeOptionalFloat(minScore);
    }

    /**
     * Returns the query builder that defines which documents the script_score query will be executed on.
     */
    public QueryBuilder query() {
        return this.query;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(QUERY_FIELD.getPreferredName());
        query.toXContent(builder, params);
        builder.field(SCRIPT_FIELD.getPreferredName(), script);
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
            Objects.equals(this.script, other.script) &&
            Objects.equals(this.minScore, other.minScore) ;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(this.query, this.script, this.minScore);
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        ScoreScript.Factory factory = context.compile(script, ScoreScript.CONTEXT);
        ScoreScript.LeafFactory scoreScriptFactory = factory.newFactory(script.getParams(), context.lookup());
        Query query = this.query.toQuery(context);
        return new ScriptScoreQuery(query, script, scoreScriptFactory, minScore,
            context.index().getName(), context.getShardId(), context.indexVersionCreated());
    }


    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        QueryBuilder newQuery = this.query.rewrite(queryRewriteContext);
        if (newQuery != query) {
            ScriptScoreQueryBuilder newQueryBuilder = new ScriptScoreQueryBuilder(newQuery, script);
            if (minScore != null) {
                newQueryBuilder.setMinScore(minScore);
            }
            return newQueryBuilder;
        }
        return this;
    }

    @Override
    protected void extractInnerHitBuilders(Map<String, InnerHitContextBuilder> innerHits) {
        InnerHitContextBuilder.extractInnerHits(query(), innerHits);
    }

}
