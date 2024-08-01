/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.FieldAttributeTests;
import org.elasticsearch.xpack.esql.plan.AbstractNodeSerializationTests;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelationSerializationTests;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractLogicalPlanSerializationTests<T extends LogicalPlan> extends AbstractNodeSerializationTests<T> {
    public static LogicalPlan randomChild(int depth) {
        if (randomBoolean() && depth < 4) {
            // TODO more random options
            return LookupSerializationTests.randomLookup(depth + 1);
        }
        // TODO more random options
        return LocalRelationSerializationTests.randomLocalRelation();
    }

    public static List<Attribute> randomFieldAttributes(int min, int max, boolean onlyRepresentable) {
        return randomList(min, max, () -> FieldAttributeTests.createFieldAttribute(0, onlyRepresentable));
    }

    @Override
    protected final NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(LogicalPlan.getNamedWriteables());
        entries.addAll(Expression.getNamedWriteables());
        entries.addAll(Attribute.getNamedWriteables());
        entries.addAll(EsField.getNamedWriteables());
        entries.addAll(Block.getNamedWriteables());
        return new NamedWriteableRegistry(entries);
    }

    @Override
    protected final Class<? extends Node<?>> categoryClass() {
        return LogicalPlan.class;
    }
}
