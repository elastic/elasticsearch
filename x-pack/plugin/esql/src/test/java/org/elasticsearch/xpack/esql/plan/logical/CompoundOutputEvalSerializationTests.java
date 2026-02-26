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
import org.elasticsearch.xpack.esql.expression.function.FieldAttributeTests;

import java.io.IOException;
import java.util.List;

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
        List<String> outputFieldNames = instance.outputFieldNames();
        List<Attribute> outputFieldAttributes = instance.generatedAttributes();

        switch (between(0, 3)) {
            case 0:
                child = randomValueOtherThan(child, () -> randomChild(0));
                break;
            case 1:
                input = randomValueOtherThan(input, () -> FieldAttributeTests.createFieldAttribute(0, false));
                break;
            case 2:
                final int nameSize = outputFieldNames.size();
                outputFieldNames = randomValueOtherThan(
                    outputFieldNames,
                    () -> randomList(nameSize, nameSize, () -> randomAlphaOfLengthBetween(1, 10))
                );
                break;
            case 3:
                final int attrSize = outputFieldAttributes.size();
                outputFieldAttributes = randomValueOtherThan(outputFieldAttributes, () -> randomFieldAttributes(attrSize, attrSize, false));
                break;
        }
        return instance.createNewInstance(instance.source(), child, input, outputFieldNames, outputFieldAttributes);
    }

    protected abstract T createInitialInstance(Source source, LogicalPlan child, Expression input, Attribute outputFieldPrefix);
}
