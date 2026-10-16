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

public class DistributedHashJoinExecSerializationTests extends AbstractPhysicalPlanSerializationTests<DistributedHashJoinExec> {
    private static DistributedHashJoinExec randomDistributedHashJoinExec(int depth) {
        Source source = randomSource();
        PhysicalPlan left = randomChild(depth);
        PhysicalPlan right = randomChild(depth);
        List<Attribute> leftFields = randomFieldAttributes(1, 5, false);
        List<Attribute> rightFields = randomFieldAttributes(1, 5, false);
        List<Attribute> addedFields = randomFieldAttributes(1, 5, false);
        return new DistributedHashJoinExec(source, left, right, leftFields, rightFields, addedFields);
    }

    @Override
    protected DistributedHashJoinExec createTestInstance() {
        return randomDistributedHashJoinExec(0);
    }

    @Override
    protected DistributedHashJoinExec mutateInstance(DistributedHashJoinExec instance) throws IOException {
        PhysicalPlan left = instance.left();
        PhysicalPlan right = instance.right();
        List<Attribute> leftFields = instance.leftFields();
        List<Attribute> rightFields = instance.rightFields();
        List<Attribute> addedFields = instance.addedFields();
        switch (between(0, 4)) {
            case 0 -> left = randomValueOtherThan(left, () -> randomChild(0));
            case 1 -> right = randomValueOtherThan(right, () -> randomChild(0));
            case 2 -> leftFields = randomValueOtherThan(leftFields, () -> randomFieldAttributes(1, 5, false));
            case 3 -> rightFields = randomValueOtherThan(rightFields, () -> randomFieldAttributes(1, 5, false));
            case 4 -> addedFields = randomValueOtherThan(addedFields, () -> randomFieldAttributes(1, 5, false));
            default -> throw new AssertionError();
        }
        return new DistributedHashJoinExec(instance.source(), left, right, leftFields, rightFields, addedFields);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
