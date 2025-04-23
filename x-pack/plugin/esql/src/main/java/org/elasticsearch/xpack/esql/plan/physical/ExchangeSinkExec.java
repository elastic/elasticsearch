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
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class ExchangeSinkExec extends UnaryExec {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "ExchangeSinkExec",
        ExchangeSinkExec::new
    );

    private final List<Attribute> output;
    // TODO: remove this flag
    private final boolean intermediateAgg;

    public ExchangeSinkExec(Source source, List<Attribute> output, boolean intermediateAgg, PhysicalPlan child) {
        super(source, child);
        this.output = output;
        this.intermediateAgg = intermediateAgg;
    }

    private ExchangeSinkExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readBoolean(),
            in.readNamedWriteable(PhysicalPlan.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteableCollection(output());
        out.writeBoolean(isIntermediateAgg());
        out.writeNamedWriteable(child());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    public boolean isIntermediateAgg() {
        return intermediateAgg;
    }

    @Override
    protected NodeInfo<? extends ExchangeSinkExec> info() {
        return NodeInfo.create(this, ExchangeSinkExec::new, output, intermediateAgg, child());
    }

    @Override
    public ExchangeSinkExec replaceChild(PhysicalPlan newChild) {
        return new ExchangeSinkExec(source(), output, intermediateAgg, newChild);
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o)) {
            ExchangeSinkExec that = (ExchangeSinkExec) o;
            return intermediateAgg == that.intermediateAgg && Objects.equals(output, that.output);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(output, intermediateAgg, child());
    }
}
