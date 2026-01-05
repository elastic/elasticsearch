/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FieldAttributeTests;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public abstract class CompoundOutputEvalSerializationTests<T extends CompoundOutputEval<T>> extends AbstractLogicalPlanSerializationTests<
    T> {

    @Override
    protected T createTestInstance() {
        Source source = randomSource();
        LogicalPlan child = randomChild(0);
        Expression input = FieldAttributeTests.createFieldAttribute(0, false);
        Attribute outputFieldPrefix = FieldAttributeTests.createFieldAttribute(0, false);
        return createInitialInstance(source, child, input, outputFieldPrefix);
    }

    @Override
    protected T mutateInstance(T instance) throws IOException {
        LogicalPlan child = instance.child();
        Expression input = instance.getInput();
        Map<String, DataType> functionOutputFields = instance.getFunctionOutputFields();
        List<Attribute> outputFields = instance.generatedAttributes();

        switch (between(0, 3)) {
            case 0:
                child = randomValueOtherThan(child, () -> randomChild(0));
                break;
            case 1:
                input = randomValueOtherThan(input, () -> FieldAttributeTests.createFieldAttribute(0, false));
                break;
            case 2:
                final int mapSize = functionOutputFields.size();
                functionOutputFields = randomValueOtherThan(functionOutputFields, () -> {
                    Map<String, DataType> newMap = new LinkedHashMap<>();
                    for (int i = 0; i < mapSize; i++) {
                        newMap.put(randomAlphaOfLength(6), randomFrom(DataType.KEYWORD, DataType.INTEGER, DataType.IP));
                    }
                    return newMap;
                });
                break;
            case 3:
                final int listSize = outputFields.size();
                outputFields = randomValueOtherThan(outputFields, () -> randomFieldAttributes(listSize, listSize, false));
                break;
        }
        return instance.createNewInstance(instance.source(), child, input, functionOutputFields, outputFields);
    }

    protected abstract T createInitialInstance(Source source, LogicalPlan child, Expression input, Attribute outputFieldPrefix);
}
