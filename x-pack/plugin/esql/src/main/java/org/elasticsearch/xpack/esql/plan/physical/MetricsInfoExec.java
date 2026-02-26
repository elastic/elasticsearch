/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Physical plan node for METRICS_INFO command.
 * <p>
 * Operates in two modes, similar to {@link AggregateExec}:
 * <ul>
 *   <li>{@link Mode#INITIAL} — runs on data nodes. Extracts metric metadata from shards
 *       ({@code _tsid}, {@code _timeseries_metadata}, mappings) and produces one row per
 *       distinct metric signature within the local shards.</li>
 *   <li>{@link Mode#FINAL} — runs on the coordinator. Receives per-data-node results via
 *       the exchange and merges rows that share the same metric signature, unioning
 *       multi-valued fields ({@code data_stream}, {@code dimension_fields}, etc.).</li>
 * </ul>
 */
public class MetricsInfoExec extends UnaryExec {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "MetricsInfoExec",
        MetricsInfoExec::new
    );

    /**
     * Execution mode, mirroring the three-phase pattern used by aggregations.
     */
    public enum Mode {
        /** Data-node phase: full shard extraction → per-node metric rows. */
        INITIAL(false, true),
        /** Coordinator phase: merge rows from all data nodes by metric signature. */
        FINAL(true, false),
        /** Node-level reduction: merges INITIAL results from multiple shards on the same data node. */
        INTERMEDIATE(true, true);

        private final boolean inputPartial;
        private final boolean outputPartial;

        Mode(boolean inputPartial, boolean outputPartial) {
            this.inputPartial = inputPartial;
            this.outputPartial = outputPartial;
        }

        /** True when this mode consumes the intermediate wire format (INTERMEDIATE, FINAL). */
        public boolean isInputPartial() {
            return inputPartial;
        }

        /** True when this mode produces the intermediate wire format (INITIAL, INTERMEDIATE). */
        public boolean isOutputPartial() {
            return outputPartial;
        }
    }

    /**
     * The output attributes of {@link Mode#INITIAL} and {@link Mode#INTERMEDIATE}, resp.
     * the input attributes of {@link Mode#FINAL} and {@link Mode#INTERMEDIATE}.
     * Analogous to {@link AggregateExec#intermediateAttributes()}.
     */
    private final List<Attribute> intermediateAttributes;
    private final List<Attribute> outputAttrs;
    private final Mode mode;

    public MetricsInfoExec(
        Source source,
        PhysicalPlan child,
        List<Attribute> outputAttrs,
        List<Attribute> intermediateAttributes,
        Mode mode
    ) {
        super(source, child);
        this.outputAttrs = outputAttrs;
        this.intermediateAttributes = intermediateAttributes;
        this.mode = mode;
    }

    private MetricsInfoExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(PhysicalPlan.class),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readEnum(Mode.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteableCollection(outputAttrs);
        out.writeNamedWriteableCollection(intermediateAttributes);
        out.writeEnum(mode);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public Mode mode() {
        return mode;
    }

    public List<Attribute> outputAttrs() {
        return outputAttrs;
    }

    public List<Attribute> intermediateAttributes() {
        return intermediateAttributes;
    }

    @Override
    protected NodeInfo<MetricsInfoExec> info() {
        return NodeInfo.create(this, MetricsInfoExec::new, child(), outputAttrs, intermediateAttributes, mode);
    }

    @Override
    public MetricsInfoExec replaceChild(PhysicalPlan newChild) {
        return new MetricsInfoExec(source(), newChild, outputAttrs, intermediateAttributes, mode);
    }

    @Override
    public List<Attribute> output() {
        return mode.isOutputPartial() ? intermediateAttributes : outputAttrs;
    }

    @Override
    protected AttributeSet computeReferences() {
        if (mode.isInputPartial()) {
            return AttributeSet.of(intermediateAttributes);
        }
        return AttributeSet.EMPTY;
    }

    @Override
    public int hashCode() {
        return Objects.hash(child(), outputAttrs, intermediateAttributes, mode);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        MetricsInfoExec other = (MetricsInfoExec) obj;
        return mode == other.mode
            && Objects.equals(child(), other.child())
            && Objects.equals(outputAttrs, other.outputAttrs)
            && Objects.equals(intermediateAttributes, other.intermediateAttributes);
    }
}
