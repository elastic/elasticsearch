/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.expression.function.ReferenceAttributeTestUtils.randomReferenceAttribute;

public class IntoExecSerializationTests extends AbstractPhysicalPlanSerializationTests<IntoExec> {
    @Override
    protected IntoExec createTestInstance() {
        Source source = randomSource();
        PhysicalPlan child = randomChild(0);
        String destination = randomAlphaOfLength(10);
        List<Attribute> sourceAttributes = randomList(0, 3, () -> randomReferenceAttribute(false));
        List<Attribute> outputAttributes = randomList(1, 3, () -> randomReferenceAttribute(false));
        IntoExec.Mode mode = randomFrom(IntoExec.Mode.values());
        return new IntoExec(source, child, destination, sourceAttributes, outputAttributes, mode);
    }

    @Override
    protected IntoExec mutateInstance(IntoExec instance) throws IOException {
        PhysicalPlan child = instance.child();
        String destination = instance.destination();
        List<Attribute> sourceAttributes = instance.sourceAttributes();
        List<Attribute> outputAttributes = instance.output();
        IntoExec.Mode mode = instance.mode();
        switch (between(0, 4)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> destination = randomValueOtherThan(destination, () -> randomAlphaOfLength(10));
            case 2 -> sourceAttributes = randomValueOtherThan(
                sourceAttributes,
                () -> randomList(0, 3, () -> randomReferenceAttribute(false))
            );
            case 3 -> outputAttributes = randomValueOtherThan(
                outputAttributes,
                () -> randomList(1, 3, () -> randomReferenceAttribute(false))
            );
            case 4 -> mode = randomValueOtherThan(mode, () -> randomFrom(IntoExec.Mode.values()));
            default -> throw new AssertionError("Unexpected case");
        }
        return new IntoExec(instance.source(), child, destination, sourceAttributes, outputAttributes, mode);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
