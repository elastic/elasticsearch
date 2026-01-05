/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FieldAttributeTests;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public abstract class CompoundOutputEvalExecSerializationTests extends AbstractPhysicalPlanSerializationTests<CompoundOutputEvalExec> {

    @Override
    protected CompoundOutputEvalExec createTestInstance() {
        Source source = randomSource();
        PhysicalPlan child = randomChild(0);
        Expression input = FieldAttributeTests.createFieldAttribute(0, false);

        int fieldCount = randomIntBetween(1, 5);
        Map<String, DataType> functionOutputFields = new LinkedHashMap<>();
        for (int i = 0; i < fieldCount; i++) {
            functionOutputFields.put(randomAlphaOfLength(5), randomFrom(DataType.KEYWORD, DataType.INTEGER, DataType.IP));
        }
        List<Attribute> outputFields = randomFieldAttributes(fieldCount, fieldCount, false);

        return createInstance(source, child, input, functionOutputFields, outputFields);
    }

    @Override
    protected CompoundOutputEvalExec mutateInstance(CompoundOutputEvalExec instance) throws IOException {
        PhysicalPlan child = instance.child();
        Expression input = instance.input();
        Map<String, DataType> functionOutputFields = instance.getFunctionOutputFields();
        List<Attribute> outputFields = instance.outputFields();

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

    protected abstract CompoundOutputEvalExec createInstance(
        Source source,
        PhysicalPlan child,
        Expression input,
        Map<String, DataType> functionOutputFields,
        List<Attribute> outputFields
    );
}
