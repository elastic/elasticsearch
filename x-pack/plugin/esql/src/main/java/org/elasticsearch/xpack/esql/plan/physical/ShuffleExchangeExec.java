/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Coordinator-only stage boundary that is split into producer and consumer plans before execution.
 */
public class ShuffleExchangeExec extends UnaryExec {
    private final List<Attribute> output;
    private final boolean inBetweenAggs;
    private final ExchangeDistribution distribution;
    private final List<Attribute> keys;

    public ShuffleExchangeExec(
        Source source,
        List<Attribute> output,
        boolean inBetweenAggs,
        ExchangeDistribution distribution,
        List<Attribute> keys,
        PhysicalPlan child
    ) {
        super(source, child);
        this.output = output;
        this.inBetweenAggs = inBetweenAggs;
        this.distribution = distribution;
        this.keys = keys;
    }

    @Override
    public List<Attribute> output() {
        return inBetweenAggs ? output : child().output();
    }

    public boolean inBetweenAggs() {
        return inBetweenAggs;
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
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new ShuffleExchangeExec(source(), output, inBetweenAggs, distribution, keys, newChild);
    }

    @Override
    protected NodeInfo<ShuffleExchangeExec> info() {
        return NodeInfo.create(this, ShuffleExchangeExec::new, output, inBetweenAggs, distribution, keys, child());
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        ShuffleExchangeExec other = (ShuffleExchangeExec) obj;
        return output.equals(other.output)
            && inBetweenAggs == other.inBetweenAggs
            && distribution == other.distribution
            && keys.equals(other.keys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(output, inBetweenAggs, distribution, keys, child());
    }
}
