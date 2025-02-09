/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.core.capabilities.Unresolvable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.telemetry.PlanTelemetry;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class UnresolvedView extends LeafPlan implements Unresolvable, TelemetryAware {
    private final String name;

    public UnresolvedView(Source source, String name) {
        super(source);
        this.name = name;
    }

    @Override
    public void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    protected NodeInfo<UnresolvedView> info() {
        return NodeInfo.create(this, UnresolvedView::new, name);
    }

    @Override
    public boolean resolved() {
        return false;
    }

    public String name() {
        return name;
    }

    /**
     *
     * This is used by {@link PlanTelemetry} to collect query statistics
     * It can return
     * <ul>
     *     <li>"FROM" if this a <code>|FROM idx</code> command</li>
     *     <li>"FROM TS" if it is the result of a <code>| METRICS idx some_aggs() BY fields</code> command</li>
     *     <li>"METRICS" if it is the result of a <code>| METRICS idx</code> (no aggs, no groupings)</li>
     * </ul>
     */
    @Override
    public String telemetryLabel() {
        return "FROM VIEW";
    }

    @Override
    public boolean expressionsResolved() {
        return false;
    }

    @Override
    public List<Attribute> output() {
        return Collections.emptyList();
    }

    @Override
    public String unresolvedMessage() {
        return "Unknown view [" + name + "]";
    }

    @Override
    public int hashCode() {
        return Objects.hash(source(), name);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        UnresolvedView other = (UnresolvedView) obj;
        return name.equals(other.name);
    }

    @Override
    public String toString() {
        return UNRESOLVED_PREFIX + name;
    }
}
