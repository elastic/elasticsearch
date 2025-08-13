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

public class HashJoinExecSerializationTests extends AbstractPhysicalPlanSerializationTests<HashJoinExec> {
    public static HashJoinExec randomHashJoinExec(int depth) {
        Source source = randomSource();
        PhysicalPlan child = randomChild(depth);
        LocalSourceExec joinData = LocalSourceExecSerializationTests.randomLocalSourceExec();
        List<Attribute> matchFields = randomFields();
        List<Attribute> leftFields = randomFields();
        List<Attribute> rightFields = randomFields();
        List<Attribute> output = randomFields();
        return new HashJoinExec(source, child, joinData, matchFields, leftFields, rightFields, output);
    }

    private static List<Attribute> randomFields() {
        return randomFieldAttributes(1, 5, false);
    }

    @Override
    protected HashJoinExec createTestInstance() {
        return randomHashJoinExec(0);
    }

    @Override
    protected HashJoinExec mutateInstance(HashJoinExec instance) throws IOException {
        PhysicalPlan child = instance.left();
        PhysicalPlan joinData = instance.joinData();
        List<Attribute> matchFields = randomFieldAttributes(1, 5, false);
        List<Attribute> leftFields = randomFieldAttributes(1, 5, false);
        List<Attribute> rightFields = randomFieldAttributes(1, 5, false);
        List<Attribute> output = randomFieldAttributes(1, 5, false);
        switch (between(0, 5)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> joinData = randomValueOtherThan(joinData, LocalSourceExecSerializationTests::randomLocalSourceExec);
            case 2 -> matchFields = randomValueOtherThan(matchFields, HashJoinExecSerializationTests::randomFields);
            case 3 -> leftFields = randomValueOtherThan(leftFields, HashJoinExecSerializationTests::randomFields);
            case 4 -> rightFields = randomValueOtherThan(rightFields, HashJoinExecSerializationTests::randomFields);
            case 5 -> output = randomValueOtherThan(output, HashJoinExecSerializationTests::randomFields);
            default -> throw new UnsupportedOperationException();
        }
        return new HashJoinExec(instance.source(), child, joinData, matchFields, leftFields, rightFields, output);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
