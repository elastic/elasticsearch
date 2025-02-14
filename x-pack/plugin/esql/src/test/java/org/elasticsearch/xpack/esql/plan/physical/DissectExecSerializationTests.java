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
import org.elasticsearch.xpack.esql.expression.function.FieldAttributeTests;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;
import org.elasticsearch.xpack.esql.plan.logical.DissectSerializationTests;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.plan.logical.AbstractLogicalPlanSerializationTests.randomFieldAttributes;

public class DissectExecSerializationTests extends AbstractPhysicalPlanSerializationTests<DissectExec> {
    public static DissectExec randomDissectExec(int depth) {
        Source source = randomSource();
        PhysicalPlan child = randomChild(depth);
        Expression inputExpression = FieldAttributeTests.createFieldAttribute(1, false);
        Dissect.Parser parser = DissectSerializationTests.randomParser();
        List<Attribute> extracted = randomFieldAttributes(0, 4, false);
        return new DissectExec(source, child, inputExpression, parser, extracted);
    }

    @Override
    protected DissectExec createTestInstance() {
        return randomDissectExec(0);
    }

    @Override
    protected DissectExec mutateInstance(DissectExec instance) throws IOException {
        PhysicalPlan child = instance.child();
        Expression inputExpression = FieldAttributeTests.createFieldAttribute(1, false);
        Dissect.Parser parser = DissectSerializationTests.randomParser();
        List<Attribute> extracted = randomFieldAttributes(0, 4, false);
        switch (between(0, 3)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> inputExpression = randomValueOtherThan(inputExpression, () -> FieldAttributeTests.createFieldAttribute(1, false));
            case 2 -> parser = randomValueOtherThan(parser, DissectSerializationTests::randomParser);
            case 3 -> extracted = randomValueOtherThan(extracted, () -> randomFieldAttributes(0, 4, false));
            default -> throw new IllegalStateException();
        }
        return new DissectExec(instance.source(), child, inputExpression, parser, extracted);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
