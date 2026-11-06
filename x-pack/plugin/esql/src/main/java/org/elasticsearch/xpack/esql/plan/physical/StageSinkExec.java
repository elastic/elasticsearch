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

/** Root of a non-coordinator stage plan that writes rows to its outgoing exchange edge. */
public class StageSinkExec extends UnaryExec {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "StageSinkExec",
        StageSinkExec::new
    );

    private final List<Attribute> output;
    private final boolean intermediateAgg;
    private final String edgeKey;
    private final ExchangeDistribution distribution;
    private final List<Attribute> keys;

    public StageSinkExec(
        Source source,
        List<Attribute> output,
        boolean intermediateAgg,
        String edgeKey,
        ExchangeDistribution distribution,
        List<Attribute> keys,
        PhysicalPlan child
    ) {
        super(source, child);
        this.output = output;
        this.intermediateAgg = intermediateAgg;
        this.edgeKey = edgeKey;
        this.distribution = distribution;
        this.keys = keys;
    }

    private StageSinkExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readBoolean(),
            in.readString(),
            in.readEnum(ExchangeDistribution.class),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readNamedWriteable(PhysicalPlan.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (distribution == ExchangeDistribution.BROADCAST
            && out.getTransportVersion().supports(ExchangeDistribution.ESQL_SHUFFLE_STAGE_EXECUTION) == false) {
            throw new IllegalStateException(
                "broadcast shuffle edges require transport version [" + ExchangeDistribution.ESQL_SHUFFLE_STAGE_EXECUTION + "]"
            );
        }
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteableCollection(output);
        out.writeBoolean(intermediateAgg);
        out.writeString(edgeKey);
        out.writeEnum(distribution);
        out.writeNamedWriteableCollection(keys);
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

    public String edgeKey() {
        return edgeKey;
    }

    public ExchangeDistribution distribution() {
        return distribution;
    }

    public List<Attribute> keys() {
        return keys;
    }

    @Override
    protected AttributeSet computeReferences() {
        return AttributeSet.EMPTY;
    }

    @Override
    public StageSinkExec replaceChild(PhysicalPlan newChild) {
        return new StageSinkExec(source(), output, intermediateAgg, edgeKey, distribution, keys, newChild);
    }

    @Override
    protected NodeInfo<StageSinkExec> info() {
        return NodeInfo.create(this, StageSinkExec::new, output, intermediateAgg, edgeKey, distribution, keys, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        StageSinkExec other = (StageSinkExec) obj;
        return output.equals(other.output)
            && intermediateAgg == other.intermediateAgg
            && edgeKey.equals(other.edgeKey)
            && distribution == other.distribution
            && keys.equals(other.keys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(output, intermediateAgg, edgeKey, distribution, keys, child());
    }
}
