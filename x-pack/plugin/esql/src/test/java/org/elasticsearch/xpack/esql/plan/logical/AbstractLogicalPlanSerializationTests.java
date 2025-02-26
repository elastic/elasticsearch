/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.compute.data.BlockWritables;
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.expression.ExpressionWritables;
import org.elasticsearch.xpack.esql.plan.AbstractNodeSerializationTests;
import org.elasticsearch.xpack.esql.plan.PlanWritables;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelationSerializationTests;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractLogicalPlanSerializationTests<T extends LogicalPlan> extends AbstractNodeSerializationTests<T> {
    public static LogicalPlan randomChild(int depth) {
        if (randomBoolean() && depth < 4) {
            // TODO more random options
            return LookupSerializationTests.randomLookup(depth + 1);
        }
        return randomBoolean() ? EsRelationSerializationTests.randomEsRelation() : LocalRelationSerializationTests.randomLocalRelation();
    }

    @Override
    protected final NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(PlanWritables.logical());
        entries.addAll(ExpressionWritables.aggregates());
        entries.addAll(ExpressionWritables.allExpressions());
        entries.addAll(BlockWritables.getNamedWriteables());
        return new NamedWriteableRegistry(entries);
    }

    @Override
    protected final Class<? extends Node<?>> categoryClass() {
        return LogicalPlan.class;
    }
}
