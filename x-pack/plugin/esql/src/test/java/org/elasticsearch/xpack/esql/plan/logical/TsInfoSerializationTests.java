/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelationSerializationTests;

import java.io.IOException;
import java.util.List;

public class TsInfoSerializationTests extends AbstractLogicalPlanSerializationTests<TsInfo> {

    /**
     * Versions that carry the limit: current, the first version on main ({@code esql_ts_info_limit}), the serverless
     * patch build, and the 9.6 and 9.5 backports ({@code esql_ts_info_limit_backport}).
     */
    public static final List<TransportVersion> LIMIT_ON_THE_WIRE = List.of(
        TransportVersion.current(),
        TransportVersion.fromId(9620000),
        TransportVersion.fromId(9616001),
        TransportVersion.fromId(9610006),
        TransportVersion.fromId(9458022)
    );

    /** The versions right before each of those, which don't carry the limit. */
    public static final List<TransportVersion> LIMIT_NOT_ON_THE_WIRE = List.of(
        TransportVersion.fromId(9619000),
        TransportVersion.fromId(9610005),
        TransportVersion.fromId(9458021)
    );

    @Override
    protected TsInfo createTestInstance() {
        return new TsInfo(randomSource(), randomChild(0), randomLimit());
    }

    @Override
    protected TsInfo mutateInstance(TsInfo instance) throws IOException {
        LogicalPlan child = instance.child();
        Integer limit = instance.limit();
        if (randomBoolean()) {
            child = randomValueOtherThan(child, () -> randomChild(0));
        } else {
            limit = randomValueOtherThan(limit, TsInfoSerializationTests::randomLimit);
        }
        return new TsInfo(instance.source(), child, limit);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }

    public void testLimitOnTheWire() throws IOException {
        TsInfo instance = new TsInfo(randomSource(), LocalRelationSerializationTests.randomLocalRelation(), between(1, 100_000));
        for (TransportVersion version : LIMIT_ON_THE_WIRE) {
            assertEquals(version.toString(), instance.limit(), copyInstance(instance, version).limit());
        }
    }

    public void testLimitNotOnTheWire() throws IOException {
        TsInfo instance = new TsInfo(randomSource(), LocalRelationSerializationTests.randomLocalRelation(), between(1, 100_000));
        for (TransportVersion version : LIMIT_NOT_ON_THE_WIRE) {
            assertNull(version.toString(), copyInstance(instance, version).limit());
        }
    }

    private static Integer randomLimit() {
        return randomBoolean() ? null : between(1, 100_000);
    }
}
