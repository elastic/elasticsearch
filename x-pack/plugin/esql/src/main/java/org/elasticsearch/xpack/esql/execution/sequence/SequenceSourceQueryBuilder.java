/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.internal.MaxClauseCountQueryVisitor;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Stops a stage query from rewriting against another source's shared-PIT shard. Timestamp bounds and field
 * predicates may be invalid for an unrelated index mapping, even when an index filter would return no hits.
 */
public final class SequenceSourceQueryBuilder extends AbstractQueryBuilder<SequenceSourceQueryBuilder> {
    public static final TransportVersion ESQL_SEQUENCE_R6_SOURCE_SCOPE = TransportVersion.fromName("esql_sequence_r6_source_scope");
    public static final String NAME = "esql_sequence_source_scope";
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        QueryBuilder.class,
        NAME,
        SequenceSourceQueryBuilder::new
    );

    private final QueryBuilder query;
    private final List<String> indices;

    /**
     * Retains the source's physical indices so unrelated shared-PIT slots stay empty before delegate rewriting.
     * Even a clause that ultimately matches nothing can otherwise parse timestamp bounds against a foreign mapping.
     */
    public SequenceSourceQueryBuilder(QueryBuilder query, List<String> indices) {
        this.query = Objects.requireNonNull(query);
        this.indices = List.copyOf(indices);
    }

    /** Restores source isolation only on peers that understand this internal query. */
    public SequenceSourceQueryBuilder(StreamInput in) throws IOException {
        super(checkSupported(in));
        query = in.readNamedWriteable(QueryBuilder.class);
        indices = in.readStringCollectionAsList();
    }

    private static StreamInput checkSupported(StreamInput in) {
        checkSupported(in.getTransportVersion());
        return in;
    }

    private static void checkSupported(TransportVersion version) {
        if (version.supports(ESQL_SEQUENCE_R6_SOURCE_SCOPE) == false) {
            throw new IllegalArgumentException("SEQUENCE source isolation is not supported by transport version [" + version + "]");
        }
    }

    /** Public Query DSL cannot create this compiler-owned validation boundary. */
    public static SequenceSourceQueryBuilder fromXContent(XContentParser parser) {
        throw new XContentParseException(parser.getTokenLocation(), "[" + NAME + "] is reserved for internal ES|QL searches");
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext context) throws IOException {
        var index = context.getFullyQualifiedIndex();
        if (index != null && indices.contains(index.getName()) == false) {
            return new MatchNoneQueryBuilder("PIT slot is outside this SEQUENCE source");
        }
        // Coordinator/own-shard rewrites retain ordinary async fetches such as terms lookup. A known foreign
        // shard must never inspect the delegate's field mappings or register one of its async actions.
        QueryBuilder rewritten = query.rewrite(context);
        return rewritten == query ? this : new SequenceSourceQueryBuilder(rewritten, indices);
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context, MaxClauseCountQueryVisitor visitor) throws IOException {
        if (indices.contains(context.index().getName()) == false) {
            return new MatchNoneQueryBuilder("PIT slot is outside this SEQUENCE source").toQuery(context, visitor);
        }
        return Rewriteable.rewrite(query, context, true).toQuery(context, visitor);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        checkSupported(out.getTransportVersion());
        out.writeNamedWriteable(query);
        out.writeStringCollection(indices);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field("query", query, params);
        builder.stringListField("indices", indices);
        boostAndQueryNameToXContent(builder);
        builder.endObject();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return ESQL_SEQUENCE_R6_SOURCE_SCOPE;
    }

    @Override
    protected boolean doEquals(SequenceSourceQueryBuilder other) {
        return query.equals(other.query) && indices.equals(other.indices);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(query, indices);
    }
}
