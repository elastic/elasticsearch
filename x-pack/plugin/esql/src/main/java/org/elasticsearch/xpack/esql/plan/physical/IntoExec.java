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
import org.elasticsearch.xpack.esql.core.tree.NodeStringMapper;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.into.IntoOptions;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Physical plan node for the {@code INTO} command.
 *
 * <p>Operates in two modes:
 * <ul>
 *   <li>{@link Mode#WRITE}: runs per driver (data node or coordinator). Reads all source columns,
 *       bulk-indexes them into the destination index, and emits one row {@code [total, created]}.</li>
 *   <li>{@link Mode#MERGE_STATS}: runs on the coordinator. Receives one {@code [total, created]} row per
 *       upstream driver and sums them into a single final row.</li>
 * </ul>
 */
public class IntoExec extends UnaryExec {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "IntoExec",
        IntoExec::new
    );

    public enum Mode {
        WRITE,
        MERGE_STATS
    }

    private final String destination;
    private final IntoOptions options;
    private final List<Attribute> sourceAttributes;
    private final List<Attribute> outputAttributes;
    private final Mode mode;

    public IntoExec(
        Source source,
        PhysicalPlan child,
        String destination,
        IntoOptions options,
        List<Attribute> sourceAttributes,
        List<Attribute> outputAttributes,
        Mode mode
    ) {
        super(source, child);
        this.destination = Objects.requireNonNull(destination);
        this.options = Objects.requireNonNull(options);
        this.sourceAttributes = sourceAttributes;
        this.outputAttributes = outputAttributes;
        this.mode = mode;
    }

    private IntoExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(PhysicalPlan.class),
            in.readString(),
            new IntoOptions(in),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readEnum(Mode.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeString(destination);
        // TODO: gate the options field on a TransportVersion together with the Into gating (see Into#postAnalysisVerification)
        options.writeTo(out);
        out.writeNamedWriteableCollection(sourceAttributes);
        out.writeNamedWriteableCollection(outputAttributes);
        out.writeEnum(mode);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public String destination() {
        return destination;
    }

    public IntoOptions options() {
        return options;
    }

    public List<Attribute> sourceAttributes() {
        return sourceAttributes;
    }

    public Mode mode() {
        return mode;
    }

    @Override
    public List<Attribute> output() {
        return outputAttributes;
    }

    @Override
    protected AttributeSet computeReferences() {
        return switch (mode) {
            case WRITE -> AttributeSet.of(sourceAttributes);
            case MERGE_STATS -> child().outputSet();
        };
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new IntoExec(source(), newChild, destination, options, sourceAttributes, outputAttributes, mode);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, IntoExec::new, child(), destination, options, sourceAttributes, outputAttributes, mode);
    }

    @Override
    public void nodeString(StringBuilder sb, NodeStringFormat format, NodeStringMapper mapper) {
        sb.append(nodeName()).append('[').append(mode).append(',').append(mapper.index(destination));
        options.appendNonDefault(sb);
        sb.append(']');
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), destination, options, sourceAttributes, outputAttributes, mode);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        IntoExec other = (IntoExec) obj;
        return destination.equals(other.destination)
            && options.equals(other.options)
            && sourceAttributes.equals(other.sourceAttributes)
            && outputAttributes.equals(other.outputAttributes)
            && mode == other.mode;
    }
}
