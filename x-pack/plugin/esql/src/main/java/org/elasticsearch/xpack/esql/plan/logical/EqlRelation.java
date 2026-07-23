/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
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
 * <p>A mapped field literally named {@code _sequence}/{@code _sequence_stage}/{@code join_keys} would collide
 * by name with a synthetic (the converter dispatches by attribute class, so values stay correct).
 */
public class EqlRelation extends LeafPlan implements TelemetryAware {

    /** EQL result mode, determined by a shallow parse of the query string at plan time. */
    public enum Mode {
        EVENT,
        SEQUENCE,
        SAMPLE
    }

    private final IndexPattern indexPattern;
    private final Expression query;
    private final Map<String, Object> options;
    private final Mode mode;
    private final List<Attribute> output;

    public EqlRelation(
        Source source,
        IndexPattern indexPattern,
        Expression query,
        Map<String, Object> options,
        Mode mode,
        List<Attribute> output
    ) {
        super(source);
        this.indexPattern = indexPattern;
        this.query = query;
        this.options = options;
        this.mode = mode;
        this.output = output;
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
        return NodeInfo.create(this, EqlRelation::new, indexPattern, query, options, mode, output);
    }

    public IndexPattern indexPattern() {
        return indexPattern;
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
        return Objects.hash(indexPattern, query, options, mode, output);
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
            && Objects.equals(output, other.output);
    }

    @Override
    public String toString() {
        return "EQL[" + query.sourceText() + "]" + output;
    }
}
