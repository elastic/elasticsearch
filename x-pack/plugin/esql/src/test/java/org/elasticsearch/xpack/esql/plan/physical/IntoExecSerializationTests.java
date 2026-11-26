/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.into.IntoOptions;
import org.elasticsearch.xpack.esql.plan.logical.IntoSerializationTests;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.expression.function.ReferenceAttributeTestUtils.randomReferenceAttribute;
import static org.elasticsearch.xpack.esql.plan.logical.IntoSerializationTests.randomIntoOptions;

public class IntoExecSerializationTests extends AbstractPhysicalPlanSerializationTests<IntoExec> {
    @Override
    protected IntoExec createTestInstance() {
        return new IntoExec(
            randomSource(),
            randomChild(0),
            randomAlphaOfLength(10),
            randomIntoOptions(),
            randomAttributes(3),
            randomAttributes(3),
            randomFrom(IntoExec.Mode.values())
        );
    }

    @Override
    protected IntoExec mutateInstance(IntoExec instance) throws IOException {
        PhysicalPlan child = instance.child();
        String destination = instance.destination();
        IntoOptions options = instance.options();
        List<Attribute> sourceAttributes = instance.sourceAttributes();
        List<Attribute> outputAttributes = instance.output();
        IntoExec.Mode mode = instance.mode();
        switch (between(0, 5)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> destination = randomValueOtherThan(destination, () -> randomAlphaOfLength(10));
            case 2 -> sourceAttributes = randomValueOtherThan(sourceAttributes, () -> randomAttributes(3));
            case 3 -> outputAttributes = randomValueOtherThan(outputAttributes, () -> randomAttributes(3));
            case 4 -> mode = randomValueOtherThan(mode, () -> randomFrom(IntoExec.Mode.values()));
            case 5 -> options = randomValueOtherThan(options, IntoSerializationTests::randomIntoOptions);
            default -> throw new AssertionError("Unexpected case");
        }
        return new IntoExec(instance.source(), child, destination, options, sourceAttributes, outputAttributes, mode);
    }

    private static List<Attribute> randomAttributes(int max) {
        return randomList(0, max, () -> randomReferenceAttribute(false));
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
