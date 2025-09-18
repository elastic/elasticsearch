/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.rescore;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.functionscore.ScriptScoreQueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.rescore.ScriptRescorer.ScriptRescoreContext;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class ScriptRescorerBuilder extends RescorerBuilder<ScriptRescorerBuilder> {
    public static final String NAME = "script";
    private static final ParseField SCRIPT_FIELD = new ParseField("script");

    private static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>(NAME, false, Builder::new);

    private static final TransportVersion SCRIPT_RESCORER = TransportVersion.fromName("script_rescorer");

    static {
        PARSER.declareObject(Builder::setScript, (p, c) -> Script.parse(p), SCRIPT_FIELD);
    }

    public static ScriptRescorerBuilder fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null).build();
    }

    private final Script script;
    private final QueryBuilder queryBuilder;

    public ScriptRescorerBuilder(Script script, QueryBuilder queryBuilder) {
        this.script = script;
        this.queryBuilder = queryBuilder;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        script.writeTo(out);
    }

    public ScriptRescorerBuilder(StreamInput in) throws IOException {
        super(in);
        this.script = new Script(in);
        this.queryBuilder = new ScriptScoreQueryBuilder(new MatchAllQueryBuilder(), script);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(SCRIPT_FIELD.getPreferredName(), script);
        builder.endObject();
    }

    @Override
    protected RescoreContext innerBuildContext(int windowSize, SearchExecutionContext context) {
        if (context.allowExpensiveQueries() == false) {
            throw new ElasticsearchException(
                "[script] queries cannot be executed when '" + SearchService.ALLOW_EXPENSIVE_QUERIES.getKey() + "' is set to false."
            );
        }
        ParsedQuery parsedQuery = context.toQuery(queryBuilder);
        ScriptRescoreContext scriptRescoreContext = new ScriptRescoreContext(parsedQuery, windowSize);
        return scriptRescoreContext;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return SCRIPT_RESCORER;
    }

    @Override
    public ScriptRescorerBuilder rewrite(QueryRewriteContext ctx) throws IOException {
        QueryBuilder rewrittenQueryBuilder = queryBuilder.rewrite(ctx);
        if (rewrittenQueryBuilder == queryBuilder) {
            return this;
        }
        ScriptRescorerBuilder rewritten = new ScriptRescorerBuilder(script, rewrittenQueryBuilder);
        if (windowSize() != null) {
            rewritten.windowSize(windowSize());
        }
        return rewritten;
    }

    static class Builder {
        private Script script;

        public void setScript(Script script) {
            this.script = script;
        }

        ScriptRescorerBuilder build() {
            QueryBuilder queryBuilder = new ScriptScoreQueryBuilder(new MatchAllQueryBuilder(), script);
            return new ScriptRescorerBuilder(script, queryBuilder);
        }
    }
}
