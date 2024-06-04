/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.List;
import java.util.Objects;

public class ShowExec extends LocalSourceExec {

    private final List<Attribute> attributes;
    private final LocalSupplier localSupplier;

    public ShowExec(Source source, List<Attribute> attributes, List<List<Object>> values) {
        this(source, attributes, LocalSupplier.of(BlockUtils.fromList(PlannerUtils.NON_BREAKING_BLOCK_FACTORY, values)));
    }

    public ShowExec(Source source, List<Attribute> attributes, LocalSupplier localSupplier) {
        super(source, attributes, localSupplier);
        this.attributes = attributes;
        this.localSupplier = localSupplier;
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, ShowExec::new, attributes, localSupplier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(attributes, localSupplier);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        return obj instanceof ShowExec other
            && Objects.equals(attributes, other.attributes)
            && Objects.equals(localSupplier, other.localSupplier);
    }
}
