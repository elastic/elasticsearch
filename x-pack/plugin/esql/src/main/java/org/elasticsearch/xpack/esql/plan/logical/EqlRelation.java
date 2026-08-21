/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.IndexPattern;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Resolved leaf for the {@code EQL <indexPattern> "<query>"} source command. Delegates execution to the EQL
 * engine (see {@code EqlSearchAction}) while exposing the results as an ES|QL table.
 *
 * <p>The output schema is the index pattern's mapping resolved through field-caps — the same shared path
 * {@code FROM} uses ({@code IndexResolver} / {@code Analyzer.mappingAsAttributes}) — so every mapped event
 * field is a typed column. Mapped fields whose type ES|QL cannot yet extract surface as {@code unsupported}
 * (same UX as {@code FROM}). Sequence and sample queries (unnested to one row per event) prepend the
 * synthetics {@code _sequence} (long, which match), {@code _sequence_stage} (integer, stage index within the
 * match) and {@code join_keys} (multivalued keyword, from the response's matched keys). {@code STATS ... BY
 * _sequence} reconstructs a match. The result mode is a shallow parse of the query string; no per-stage
 * analysis is required.
 *
 * <p>A declared {@code METADATA} clause appends provenance columns last (after the mapped fields): only
 * {@code _index}, {@code _id} and {@code _source}, populated from the EQL response envelope.
 *
 * <p>Under {@code SET unmapped_fields}, {@code ResolveUnmapped} appends columns for fields referenced by
 * downstream ES|QL but absent from the mapping: {@code NULLIFY} adds a {@code NULL}-typed column (constant nulls);
 * {@code LOAD} adds a keyword column fetched from {@code _source} via the fields API. Field references inside the
 * EQL query string itself are the EQL engine's concern (its {@code ?field} optional syntax), not this mechanism.
 *
 * <p>A mapped field literally named {@code _sequence}/{@code _sequence_stage}/{@code join_keys} (or a metadata
 * name) would collide by name with a synthetic or metadata column (the converter dispatches by attribute
 * class, so values stay correct).
 */
public class EqlRelation extends LeafPlan {

    /**
     * EQL result mode, determined by a shallow parse of the query string at plan time. This is the plan-node-owned
     * enum that travels with the physical plan ({@code EqlSourceExec} serializes it via {@code writeEnum}). It is
     * derived from {@code EqlQueryMode} — the eql-module facade that confines the {@code ql}/{@code eql} parse-tree
     * types — and kept deliberately distinct from it: this one is an ES|QL plan concern, that one an EQL-engine boundary.
     */
    public enum Mode {
        EVENT,
        SEQUENCE,
        SAMPLE
    }

    public static final String SEQUENCE_COLUMN = "_sequence";
    public static final String SEQUENCE_STAGE_COLUMN = "_sequence_stage";
    public static final String JOIN_KEYS_COLUMN = "join_keys";

    /**
     * The synthetic columns a sequence/sample result prepends, in schema order: {@code _sequence} (long, which
     * match a row belongs to), {@code _sequence_stage} (integer, stage index) and {@code join_keys} (keyword,
     * the match's join keys). Empty for {@link Mode#EVENT}. This is the single definition of the synthetic
     * schema — {@code EqlPageConverter} dispatches on the same name constants, so the two cannot drift.
     */
    public static List<Attribute> syntheticColumns(Source source, Mode mode) {
        return mode == Mode.EVENT
            ? List.of()
            : List.of(
                new ReferenceAttribute(source, SEQUENCE_COLUMN, DataType.LONG),
                new ReferenceAttribute(source, SEQUENCE_STAGE_COLUMN, DataType.INTEGER),
                new ReferenceAttribute(source, JOIN_KEYS_COLUMN, DataType.KEYWORD)
            );
    }

    private final IndexPattern indexPattern;
    private final Expression query;
    private final Map<String, Object> options;
    private final Mode mode;
    private final List<Attribute> output;
    /**
     * The row {@code LIMIT} folded into the EQL request {@code size} by {@code PushLimitIntoEqlRelation}, or
     * {@code null} when no limit was pushed. Applies in every mode: {@code size} bounds the number of matches and
     * each match unnests to at least one row, so a pushed size can never yield fewer rows than the retained
     * {@code LIMIT} trims to. Null means the request falls back to the ES|QL result-truncation cap.
     */
    private final Integer pushedLimit;
    /**
     * The merged field-caps ES|QL already resolved for this pattern, so the delegated EQL search reuses it instead of
     * re-resolving (see {@code EqlRequests}), or {@code null} when nothing was retained. Coordinator-local: never
     * serialized, and excluded from {@link #equals}/{@link #hashCode} — it is a resolution ES|QL happens to have in hand,
     * redundant with the EQL engine re-resolving it, not part of the plan's meaning.
     */
    @Nullable
    private final FieldCapabilitiesResponse preResolvedFieldCaps;

    public EqlRelation(
        Source source,
        IndexPattern indexPattern,
        Expression query,
        Map<String, Object> options,
        Mode mode,
        List<Attribute> output
    ) {
        this(source, indexPattern, query, options, mode, output, null, null);
    }

    public EqlRelation(
        Source source,
        IndexPattern indexPattern,
        Expression query,
        Map<String, Object> options,
        Mode mode,
        List<Attribute> output,
        Integer pushedLimit,
        @Nullable FieldCapabilitiesResponse preResolvedFieldCaps
    ) {
        super(source);
        this.indexPattern = indexPattern;
        this.query = query;
        this.options = options;
        this.mode = mode;
        this.output = output;
        this.pushedLimit = pushedLimit;
        this.preResolvedFieldCaps = preResolvedFieldCaps;
    }

    /** Returns a copy with the row {@code LIMIT} folded into the request size (see {@link #pushedLimit}). */
    public EqlRelation withPushedLimit(int pushedLimit) {
        return new EqlRelation(source(), indexPattern, query, options, mode, output, pushedLimit, preResolvedFieldCaps);
    }

    /**
     * Returns a copy with a replaced output schema — used by {@code ResolveUnmapped} to append unmapped-field columns
     * (nullified or {@code _source}-loaded) under {@code SET unmapped_fields}. Mirrors {@code EsRelation.withAttributes}.
     */
    public EqlRelation withAttributes(List<Attribute> newOutput) {
        return new EqlRelation(source(), indexPattern, query, options, mode, newOutput, pushedLimit, preResolvedFieldCaps);
    }

    @Override
    public void writeTo(StreamOutput out) {
        // Coordinator-local: the EQL source is executed on the coordinating node and is never shipped
        // to data nodes, so this logical node does not cross the wire.
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    protected NodeInfo<EqlRelation> info() {
        return NodeInfo.create(this, EqlRelation::new, indexPattern, query, options, mode, output, pushedLimit, preResolvedFieldCaps);
    }

    public IndexPattern indexPattern() {
        return indexPattern;
    }

    public Integer pushedLimit() {
        return pushedLimit;
    }

    @Nullable
    public FieldCapabilitiesResponse preResolvedFieldCaps() {
        return preResolvedFieldCaps;
    }

    public Expression query() {
        return query;
    }

    public Map<String, Object> options() {
        return options;
    }

    public Mode mode() {
        return mode;
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
    public int hashCode() {
        return Objects.hash(indexPattern, query, options, mode, output, pushedLimit);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        EqlRelation other = (EqlRelation) obj;
        return Objects.equals(indexPattern, other.indexPattern)
            && Objects.equals(query, other.query)
            && Objects.equals(options, other.options)
            && mode == other.mode
            && Objects.equals(output, other.output)
            && Objects.equals(pushedLimit, other.pushedLimit);
    }

    @Override
    public String toString() {
        return "EQL[" + query.sourceText() + "]" + output;
    }
}
