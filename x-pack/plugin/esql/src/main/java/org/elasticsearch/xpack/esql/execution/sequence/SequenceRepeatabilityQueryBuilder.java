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
 * Validates sequence source repeatability before a shard rewrites or executes its candidate/replay query. A PIT
 * freezes document visibility, not runtime scripts. Keeping the wrapper through match-none rewrites also validates
 * zero-hit inputs; the raw timestamp and demanded fields are checked even when projections hide their values.
 */
public final class SequenceRepeatabilityQueryBuilder extends AbstractQueryBuilder<SequenceRepeatabilityQueryBuilder> {
    public static final TransportVersion ESQL_SEQUENCE_REPEATABLE_INPUT = TransportVersion.fromName("esql_sequence_repeatable_input");
    public static final String NAME = "esql_sequence_repeatable_input";
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        QueryBuilder.class,
        NAME,
        SequenceRepeatabilityQueryBuilder::new
    );

    private final QueryBuilder query;
    private final List<String> fields;
    private final List<String> indices;

    /** Retains the source's physical indices so unrelated shared-PIT slots remain empty without reading their mappings. */
    public SequenceRepeatabilityQueryBuilder(QueryBuilder query, List<String> fields, List<String> indices) {
        this.query = Objects.requireNonNull(query);
        this.fields = List.copyOf(fields);
        this.indices = List.copyOf(indices);
    }

    /** Restores the complete guard only on peers that implement the same fail-closed source contract. */
    public SequenceRepeatabilityQueryBuilder(StreamInput in) throws IOException {
        super(checkSupported(in));
        query = in.readNamedWriteable(QueryBuilder.class);
        fields = in.readStringCollectionAsList();
        indices = in.readStringCollectionAsList();
    }

    private static StreamInput checkSupported(StreamInput in) {
        checkSupported(in.getTransportVersion());
        return in;
    }

    private static void checkSupported(TransportVersion version) {
        if (version.supports(ESQL_SEQUENCE_REPEATABLE_INPUT) == false) {
            throw new IllegalArgumentException(
                "SEQUENCE repeatable source evaluation is not supported by transport version [" + version + "]"
            );
        }
    }

    /** Public Query DSL cannot create this compiler-owned validation boundary. */
    public static SequenceRepeatabilityQueryBuilder fromXContent(XContentParser parser) {
        throw new XContentParseException(parser.getTokenLocation(), "[" + NAME + "] is reserved for internal ES|QL searches");
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext context) {
        // The shard must validate mappings and install the repeatability guard before any delegate rewrite can
        // remove an unsupported dependency or resolve an external lookup. In particular, do not unwrap match-none.
        return this;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context, MaxClauseCountQueryVisitor visitor) throws IOException {
        if (indices.contains(context.index().getName())) {
            context.requireRepeatableEvaluation();
            context.getFieldType("@timestamp");
            for (String field : fields) {
                context.getFieldType(field);
            }
        }
        return Rewriteable.rewrite(query, context, true).toQuery(context, visitor);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        checkSupported(out.getTransportVersion());
        out.writeNamedWriteable(query);
        out.writeStringCollection(fields);
        out.writeStringCollection(indices);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field("query", query, params);
        builder.stringListField("fields", fields);
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
        return ESQL_SEQUENCE_REPEATABLE_INPUT;
    }

    @Override
    protected boolean doEquals(SequenceRepeatabilityQueryBuilder other) {
        return query.equals(other.query) && fields.equals(other.fields) && indices.equals(other.indices);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(query, fields, indices);
    }
}
