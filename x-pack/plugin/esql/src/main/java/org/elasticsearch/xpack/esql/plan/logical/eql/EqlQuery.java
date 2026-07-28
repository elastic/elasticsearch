/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.eql;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;

/**
 * Source command that delegates to the EQL search endpoint.
 * <p>
 * Unlike {@code FROM}, this command does not read from Elasticsearch indices through the ES|QL
 * compute engine. Instead it forwards the (index-pattern, EQL-query) pair to the EQL transport
 * action at execution time, then flattens the {@code EqlSearchResponse} into ES|QL rows. Because
 * EQL results are assembled on the coordinator (EQL has no compute engine) this node executes
 * coordinator-only and is never shipped to data nodes.
 * <p>
 * The output schema is <b>fixed</b> and does not depend on the fields matched by the EQL query, so
 * that it is known at analysis/planning time (a hard requirement for the ES|QL planner):
 * <ul>
 *   <li>{@code _sequence} ({@code long}) &mdash; 0-based ordinal of the matched sequence or sample (both are
 *       returned as sequences by the EQL response); {@code null} for plain event queries.</li>
 *   <li>{@code _index} ({@code keyword}) &mdash; the source index of the event.</li>
 *   <li>{@code _id} ({@code keyword}) &mdash; the {@code _id} of the event.</li>
 *   <li>{@code _source} ({@code keyword}) &mdash; the raw {@code _source} JSON of the event.</li>
 * </ul>
 * Typed per-field projection is intentionally out of scope for this initial version; downstream
 * pipes can extract fields from {@code _source} (e.g. via {@code DISSECT}/{@code GROK}).
 * <p>
 * An optional {@code limit} carries an ES|QL {@code LIMIT} that sits directly above this source
 * (see {@code PushDownLimitToEqlQuery}); it is forwarded to the EQL request as {@code size} so the
 * EQL endpoint can bound the number of returned events/sequences. It is only a hint: the downstream
 * {@code Limit} still enforces the exact ES|QL row count.
 */
public class EqlQuery extends LeafPlan implements TelemetryAware, ExecutesOn.Coordinator {

    public static final String SEQUENCE_FIELD = "_sequence";
    public static final String INDEX_FIELD = "_index";
    public static final String ID_FIELD = "_id";
    public static final String SOURCE_FIELD = "_source";

    private final String index;
    private final String query;
    private final EqlQueryOptions options;
    private final List<Attribute> output;
    @Nullable
    private final Integer limit;

    public EqlQuery(Source source, String index, String query) {
        this(source, index, query, EqlQueryOptions.DEFAULTS);
    }

    public EqlQuery(Source source, String index, String query, EqlQueryOptions options) {
        this(source, index, query, options, defaultOutput(source), null);
    }

    public EqlQuery(Source source, String index, String query, EqlQueryOptions options, List<Attribute> output, @Nullable Integer limit) {
        super(source);
        this.index = index;
        this.query = query;
        this.options = options;
        this.output = output;
        this.limit = limit;
    }

    /**
     * Builds the fixed output schema. Fresh {@link org.elasticsearch.xpack.esql.core.expression.NameId}s are
     * minted per instance; the list is carried across plan copies (see {@link #info()}) so downstream
     * attribute references stay bound to the same ids.
     */
    private static List<Attribute> defaultOutput(Source source) {
        return List.of(
            new ReferenceAttribute(source, null, SEQUENCE_FIELD, LONG, Nullability.TRUE, null, false),
            new ReferenceAttribute(source, null, INDEX_FIELD, KEYWORD, Nullability.TRUE, null, false),
            new ReferenceAttribute(source, null, ID_FIELD, KEYWORD, Nullability.TRUE, null, false),
            new ReferenceAttribute(source, null, SOURCE_FIELD, KEYWORD, Nullability.TRUE, null, false)
        );
    }

    public String index() {
        return index;
    }

    public String query() {
        return query;
    }

    public EqlQueryOptions options() {
        return options;
    }

    /**
     * The row limit pushed down from a directly-following ES|QL {@code LIMIT}, forwarded to the EQL request as
     * {@code size}; {@code null} when no limit was pushed (EQL then applies its own default size).
     */
    @Nullable
    public Integer limit() {
        return limit;
    }

    public EqlQuery withLimit(int limit) {
        return new EqlQuery(source(), index, query, options, output, limit);
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    public String telemetryLabel() {
        return "EQL";
    }

    @Override
    public void writeTo(StreamOutput out) {
        // Coordinator-only, snapshot-gated command: it is never serialized to data nodes.
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, EqlQuery::new, index, query, options, output, limit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, query, options, output, limit);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        EqlQuery other = (EqlQuery) obj;
        return Objects.equals(index, other.index)
            && Objects.equals(query, other.query)
            && Objects.equals(options, other.options)
            && Objects.equals(output, other.output)
            && Objects.equals(limit, other.limit);
    }
}
