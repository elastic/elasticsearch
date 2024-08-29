/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.plan.AbstractNodeSerializationTests;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.plan.physical.AggregateExecSerializationTests.randomAggregateExec;
import static org.elasticsearch.xpack.esql.plan.physical.DissectExecSerializationTests.randomDissectExec;
import static org.elasticsearch.xpack.esql.plan.physical.EsSourceExecSerializationTests.randomEsSourceExec;

public abstract class AbstractPhysicalPlanSerializationTests<T extends PhysicalPlan> extends AbstractNodeSerializationTests<T> {
    public static PhysicalPlan randomChild(int depth) {
        if (randomBoolean() && depth < 4) {
            // TODO more random options
            return randomBoolean() ? randomDissectExec(depth + 1) : randomAggregateExec(depth + 1);
        }
        return randomEsSourceExec();
    }

    public static Integer randomEstimatedRowSize() {
        return randomBoolean() ? null : between(0, Integer.MAX_VALUE);
    }

    @Override
    protected final NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(PhysicalPlan.getNamedWriteables());
        entries.addAll(AggregateFunction.getNamedWriteables());
        entries.addAll(Expression.getNamedWriteables());
        entries.addAll(Attribute.getNamedWriteables());
        entries.addAll(Block.getNamedWriteables());
        entries.addAll(NamedExpression.getNamedWriteables());
        entries.addAll(new SearchModule(Settings.EMPTY, List.of()).getNamedWriteables()); // Query builders
        entries.add(Add.ENTRY); // Used by the eval tests
        return new NamedWriteableRegistry(entries);
    }

    @Override
    protected final Class<? extends Node<?>> categoryClass() {
        return PhysicalPlan.class;
    }
}
