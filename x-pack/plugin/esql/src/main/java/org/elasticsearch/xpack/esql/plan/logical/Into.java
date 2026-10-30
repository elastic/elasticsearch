/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.into.IntoOptions;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Names the destination a query's rows are written to, as in {@code FROM source | ... | INTO destination}. Such a
 * query returns how many rows it wrote rather than the rows.
 * <p>
 * Always the root of the query it ends. The session takes it off right after parsing and runs the child as the
 * query, so it is never analyzed, optimized or serialized, and is not registered with the
 * {@code NamedWriteableRegistry}.
 */
public class Into extends UnaryPlan implements TelemetryAware {

    private final String destination;
    private final IntoOptions options;

    public Into(Source source, LogicalPlan child, String destination, IntoOptions options) {
        super(source, child);
        this.destination = Objects.requireNonNull(destination);
        this.options = Objects.requireNonNull(options);
    }

    public String destination() {
        return destination;
    }

    public IntoOptions options() {
        return options;
    }

    /**
     * Refuses a query whose rows are sorted without a limit. The order rows are written in is not something the
     * destination keeps, and since an {@code INTO} query is not capped by the implicit result limit, sorting would
     * mean holding the whole result.
     */
    public static void verifyProducer(LogicalPlan producer, String destination) {
        refuseUnlimitedSort(producer, false, destination);
    }

    private static void refuseUnlimitedSort(LogicalPlan plan, boolean limited, String destination) {
        if (plan instanceof OrderBy && limited == false) {
            throw new VerificationException(
                "[{}]: SORT has no effect on what INTO writes; remove it, or add an explicit LIMIT",
                destination
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
        return new Into(source(), newChild, destination, options);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Into::new, child(), destination, options);
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
        return Objects.hash(child(), destination, options);
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
        return Objects.equals(child(), other.child()) && destination.equals(other.destination) && options.equals(other.options);
    }
}
