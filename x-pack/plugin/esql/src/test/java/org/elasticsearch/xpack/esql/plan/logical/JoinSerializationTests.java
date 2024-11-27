/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinConfig;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinType;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;

import java.io.IOException;
import java.util.List;

public class JoinSerializationTests extends AbstractLogicalPlanSerializationTests<Join> {
    @Override
    protected Join createTestInstance() {
        Source source = randomSource();
        LogicalPlan left = randomChild(0);
        LogicalPlan right = randomChild(0);
        JoinConfig config = randomJoinConfig();
        return new Join(source, left, right, config);
    }

    private static JoinConfig randomJoinConfig() {
        JoinType type = randomFrom(JoinTypes.LEFT, JoinTypes.RIGHT, JoinTypes.INNER, JoinTypes.FULL, JoinTypes.CROSS);
        List<Attribute> matchFields = randomFieldAttributes(1, 10, false);
        List<Attribute> leftFields = randomFieldAttributes(1, 10, false);
        List<Attribute> rightFields = randomFieldAttributes(1, 10, false);
        return new JoinConfig(type, matchFields, leftFields, rightFields);
    }

    @Override
    protected Join mutateInstance(Join instance) throws IOException {
        LogicalPlan left = instance.left();
        LogicalPlan right = instance.right();
        JoinConfig config = instance.config();
        switch (between(0, 2)) {
            case 0 -> left = randomValueOtherThan(left, () -> randomChild(0));
            case 1 -> right = randomValueOtherThan(right, () -> randomChild(0));
            case 2 -> config = randomValueOtherThan(config, JoinSerializationTests::randomJoinConfig);
        }
        return new Join(instance.source(), left, right, config);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
