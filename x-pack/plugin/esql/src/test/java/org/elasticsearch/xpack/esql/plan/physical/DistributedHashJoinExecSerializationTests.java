/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.TransportVersionUtils;
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

    public void testBroadcastModeRequiresItsTransportVersion() throws IOException {
        DistributedHashJoinExec join = randomDistributedHashJoinExec(0);
        DistributedHashJoinExec broadcastJoin = new DistributedHashJoinExec(
            join.source(),
            join.left(),
            join.right(),
            join.leftFields(),
            join.rightFields(),
            join.addedFields(),
            DistributedHashJoinExec.Mode.BROADCAST
        );

        TransportVersion oldVersion = TransportVersionUtils.getPreviousVersion(ExchangeDistribution.ESQL_SHUFFLE_STAGE_EXECUTION);
        assertEquals(broadcastJoin, copyInstance(broadcastJoin, ExchangeDistribution.ESQL_SHUFFLE_STAGE_EXECUTION));
        assertEquals(
            "older transport versions decode the legacy join format as partitioned",
            DistributedHashJoinExec.Mode.PARTITIONED,
            copyInstance(join, oldVersion).mode()
        );
        expectThrows(IllegalStateException.class, () -> copyInstance(broadcastJoin, oldVersion));
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
        DistributedHashJoinExec.Mode mode = instance.mode();
        switch (between(0, 5)) {
            case 0 -> left = randomValueOtherThan(left, () -> randomChild(0));
            case 1 -> right = randomValueOtherThan(right, () -> randomChild(0));
            case 2 -> leftFields = randomValueOtherThan(leftFields, () -> randomFieldAttributes(1, 5, false));
            case 3 -> rightFields = randomValueOtherThan(rightFields, () -> randomFieldAttributes(1, 5, false));
            case 4 -> addedFields = randomValueOtherThan(addedFields, () -> randomFieldAttributes(1, 5, false));
            case 5 -> mode = randomValueOtherThan(mode, () -> randomFrom(DistributedHashJoinExec.Mode.values()));
            default -> throw new AssertionError();
        }
        return new DistributedHashJoinExec(instance.source(), left, right, leftFields, rightFields, addedFields, mode);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
