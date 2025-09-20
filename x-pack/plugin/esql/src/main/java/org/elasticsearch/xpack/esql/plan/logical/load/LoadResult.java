/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.load;

import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;

import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;

/**
 * Logical node for LOAD_RESULT {@code "<id>"}. It is translated in pre-mapper into a LocalRelation with the result rows.
 */
public class LoadResult extends LeafPlan implements TelemetryAware {

    private final String asyncId;

    public LoadResult(Source source, String asyncId) {
        super(source);
        this.asyncId = asyncId;
    }

    public String asyncId() {
        return asyncId;
    }

    @Override
    public List<Attribute> output() {
        // Placeholder; replaced by PreMapper with actual schema. Not serialized.
        return List.of(new ReferenceAttribute(Source.EMPTY, null, "_placeholder", KEYWORD));
    }

    @Override
    public String telemetryLabel() {
        return "LOAD_RESULT";
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    public void writeTo(org.elasticsearch.common.io.stream.StreamOutput out) {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this);
    }

    @Override
    public int hashCode() {
        return asyncId.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj || (obj instanceof LoadResult other && asyncId.equals(other.asyncId));
    }
}


