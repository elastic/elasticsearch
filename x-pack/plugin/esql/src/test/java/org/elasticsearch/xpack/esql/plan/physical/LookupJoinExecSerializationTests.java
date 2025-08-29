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

public class LookupJoinExecSerializationTests extends AbstractPhysicalPlanSerializationTests<LookupJoinExec> {
    public static LookupJoinExec randomLookupJoinExec(int depth) {
        Source source = randomSource();
        PhysicalPlan child = randomChild(depth);
        PhysicalPlan lookup = randomChild(depth);
        List<Attribute> leftFields = randomFields();
        List<Attribute> rightFields = randomFields();
        List<Attribute> addedFields = randomFields();
        return new LookupJoinExec(source, child, lookup, leftFields, rightFields, addedFields);
    }

    private static List<Attribute> randomFields() {
        return randomFieldAttributes(1, 5, false);
    }

    @Override
    protected LookupJoinExec createTestInstance() {
        return randomLookupJoinExec(0);
    }

    @Override
    protected LookupJoinExec mutateInstance(LookupJoinExec instance) throws IOException {
        PhysicalPlan child = instance.left();
        PhysicalPlan lookup = instance.lookup();
        List<Attribute> leftFields = randomFields();
        List<Attribute> rightFields = randomFields();
        List<Attribute> addedFields = randomFields();
        switch (between(0, 4)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> lookup = randomValueOtherThan(lookup, () -> randomChild(0));
            case 2 -> leftFields = randomValueOtherThan(leftFields, LookupJoinExecSerializationTests::randomFields);
            case 3 -> rightFields = randomValueOtherThan(rightFields, LookupJoinExecSerializationTests::randomFields);
            case 4 -> addedFields = randomValueOtherThan(addedFields, LookupJoinExecSerializationTests::randomFields);
            default -> throw new UnsupportedOperationException();
        }
        return new LookupJoinExec(instance.source(), child, lookup, leftFields, rightFields, addedFields);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
