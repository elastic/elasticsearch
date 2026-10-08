/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Names the destination a query's rows are written to, as in {@code FROM source | ... | INTO destination}. Such a
 * query returns how many rows it wrote rather than the rows.
 * <p>
 * Resolved and written on the coordinator, so it is never serialized and not registered with the
 * {@code NamedWriteableRegistry}.
 */
public class Into extends UnaryPlan {

    private final String destination;

    public Into(Source source, LogicalPlan child, String destination) {
        super(source, child);
        this.destination = Objects.requireNonNull(destination);
    }

    public String destination() {
        return destination;
    }

    /**
     * Refuses a query whose rows are sorted without a limit. The order rows are written in is not something the
     * destination keeps, and sorting them all would mean holding the whole result.
     */
    public static void verifyProducer(LogicalPlan producer, String destination) {
        refuseUnlimitedSort(producer, false, destination);
    }

    private static void refuseUnlimitedSort(LogicalPlan plan, boolean limited, String destination) {
        if (plan instanceof OrderBy && limited == false) {
            throw new IllegalArgumentException(
                "[" + destination + "]: SORT has no effect on what INTO writes; remove it, or add an explicit LIMIT"
            );
        }
        final boolean limitedBelow = limited || plan instanceof Limit;
        for (LogicalPlan child : plan.children()) {
            refuseUnlimitedSort(child, limitedBelow, destination);
        }
    }

    @Override
    public List<Attribute> output() {
        return child().output();
    }

    @Override
    public boolean expressionsResolved() {
        return child().expressionsResolved();
    }

    @Override
    public Into replaceChild(LogicalPlan newChild) {
        return new Into(source(), newChild, destination);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Into::new, child(), destination);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("INTO is resolved on the coordinator and never serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("INTO is resolved on the coordinator and never serialized");
    }

    @Override
    public int hashCode() {
        return Objects.hash(child(), destination);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Into other = (Into) obj;
        return Objects.equals(child(), other.child()) && Objects.equals(destination, other.destination);
    }
}
