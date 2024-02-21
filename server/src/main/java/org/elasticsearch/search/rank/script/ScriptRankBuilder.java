/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank.script;

import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.rank.RankBuilder;
import org.elasticsearch.search.rank.RankCoordinatorContext;
import org.elasticsearch.search.rank.RankShardContext;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ScriptRankBuilder extends RankBuilder { // TODO: This needs to become a rewritable

    public static ScriptRankBuilder fromXContent(XContentParser parser) {
        throw new UnsupportedOperationException("Use Retrievers instead");
    }

    private final Script script;
    private final List<String> fields;
    private List<QueryBuilder> queries;

    public ScriptRankBuilder(int windowSize, Script script, List<String> fields, List<QueryBuilder> queries) {
        super(windowSize);
        this.script = Objects.requireNonNull(script);
        this.fields = Objects.requireNonNull(fields);
        this.queries = Objects.requireNonNull(queries);
    }

    public ScriptRankBuilder(StreamInput in) throws IOException {
        super(in);
        this.script = new Script(in);
        this.fields = in.readStringCollectionAsList();
        this.queries = in.readNamedWriteableCollectionAsList(QueryBuilder.class);
    }

    @Override
    public String getWriteableName() {
        return ScriptRankRetrieverBuilder.NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.SCRIPT_RANK_ADDED;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        this.script.writeTo(out);
        out.writeStringCollection(fields);
        out.writeNamedWriteableCollection(queries);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        throw new UnsupportedOperationException("TODO"); // Todo
    }

    @Override
    public RankShardContext buildRankShardContext(List<Query> queries, int from) {
        return new ScriptRankShardContext(queries, from, windowSize());
    }

    @Override
    public RankCoordinatorContext buildRankCoordinatorContext(int size, int from, ScriptService scriptService) {
        return new ScriptRankCoordinatorContext(size, from, windowSize(), scriptService, script);
    }

    @Override
    public FetchSubPhaseProcessor buildFetchSubPhaseProcessor(FetchContext fetchContext) {
        return new ScriptRankFetchSubPhaseProcessor(fetchContext, fields, queries);
    }

    @Override
    protected boolean doEquals(RankBuilder other) {
        return Objects.equals(script, ((ScriptRankBuilder) other).script)
            && Objects.equals(fields, ((ScriptRankBuilder) other).fields)
            && Objects.equals(queries, ((ScriptRankBuilder) other).queries);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(script, fields, queries);
    }

    @Override
    public ScriptRankBuilder rewrite(QueryRewriteContext ctx) throws IOException {
        List<QueryBuilder> rewrittenQueries = new ArrayList<>();
        for (var query : queries) {
            rewrittenQueries.add(Rewriteable.rewrite(query, ctx));
        }
        if (rewrittenQueries.equals(queries) == false) {
            return new ScriptRankBuilder(windowSize(), script, fields, rewrittenQueries);
        }
        return this;
    }
}
