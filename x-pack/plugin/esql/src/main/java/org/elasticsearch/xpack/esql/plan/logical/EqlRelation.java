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
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Resolved leaf for the {@code EQL "<query>"} source command. Delegates execution to the EQL engine
 * (see {@code EqlSearchAction}) while exposing the results under a fixed, columnar schema that ES|QL
 * fixes at planning time.
 *
 * <p>EQL results are document-shaped and mode-dependent, so this node projects them onto one of two
 * fixed schemas, chosen solely by the query's {@link Mode} (a shallow parse; no per-stage analysis):
 * <ul>
 *   <li><b>Event queries</b> ({@link Mode#EVENT}) — one row per matched event:
 *       {@code _index} (keyword), {@code _id} (keyword), {@code _source} ({@link DataType#SOURCE}).</li>
 *   <li><b>Sequence / sample queries</b> ({@link Mode#SEQUENCE} / {@link Mode#SAMPLE}) — <em>unnested</em>
 *       to one row per event, so the schema is fixed regardless of how many stages the query has:
 *       {@code _seq} (long, which match), {@code _position} (int, stage index within the match),
 *       {@code join_keys} (multivalued keyword), then {@code _index}, {@code _id}, {@code _source}
 *       for the event at that position. Downstream {@code STATS ... BY _seq} reconstructs a match.</li>
 * </ul>
 * The event payload is intentionally opaque ({@link DataType#SOURCE}) so the schema is fully
 * determined by the mode — no field-caps resolution and no stage counting are required.
 */
public class EqlRelation extends LeafPlan implements TelemetryAware {

    /** EQL result mode, determined by a shallow parse of the query string at plan time. */
    public enum Mode {
        EVENT,
        SEQUENCE,
        SAMPLE
    }

    private final Expression query;
    private final Map<String, Object> options;
    private final Mode mode;
    private final List<Attribute> output;

    public EqlRelation(Source source, Expression query, Map<String, Object> options, Mode mode) {
        this(source, query, options, mode, buildOutput(source, mode));
    }

    public EqlRelation(Source source, Expression query, Map<String, Object> options, Mode mode, List<Attribute> output) {
        super(source);
        this.query = query;
        this.options = options;
        this.mode = mode;
        this.output = output;
    }

    /**
     * Builds the fixed output schema for the given result mode. See the class javadoc for the column
     * layout. Attributes are plain {@link ReferenceAttribute}s (regular, visible output columns) rather
     * than {@code MetadataAttribute}s, which the analyzer would otherwise hide from the default output.
     */
    public static List<Attribute> buildOutput(Source source, Mode mode) {
        List<Attribute> attrs = new ArrayList<>();
        if (mode == Mode.EVENT) {
            attrs.add(new ReferenceAttribute(source, "_index", DataType.KEYWORD));
            attrs.add(new ReferenceAttribute(source, "_id", DataType.KEYWORD));
            attrs.add(new ReferenceAttribute(source, "_source", DataType.SOURCE));
        } else {
            attrs.add(new ReferenceAttribute(source, "_seq", DataType.LONG));
            attrs.add(new ReferenceAttribute(source, "_position", DataType.INTEGER));
            // Named "join_keys" (matching EQL's response field) rather than "by": "by" is a reserved ES|QL
            // keyword, so a "by" column could not be referenced downstream (KEEP/SORT/...) without backticks.
            attrs.add(new ReferenceAttribute(source, "join_keys", DataType.KEYWORD));
            attrs.add(new ReferenceAttribute(source, "_index", DataType.KEYWORD));
            attrs.add(new ReferenceAttribute(source, "_id", DataType.KEYWORD));
            attrs.add(new ReferenceAttribute(source, "_source", DataType.SOURCE));
        }
        return attrs;
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
        return NodeInfo.create(this, EqlRelation::new, query, options, mode, output);
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
        return Objects.hash(query, options, mode, output);
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
        return Objects.equals(query, other.query)
            && Objects.equals(options, other.options)
            && mode == other.mode
            && Objects.equals(output, other.output);
    }

    @Override
    public String toString() {
        return "EQL[" + query.sourceText() + "]" + output;
    }
}
