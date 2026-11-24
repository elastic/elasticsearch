/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.xpack.esql.core.expression.Attribute;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.plan.logical.TsInfoSerializationTests.LIMIT_NOT_ON_THE_WIRE;
import static org.elasticsearch.xpack.esql.plan.logical.TsInfoSerializationTests.LIMIT_ON_THE_WIRE;

public class TsInfoExecSerializationTests extends AbstractPhysicalPlanSerializationTests<TsInfoExec> {

    @Override
    protected TsInfoExec createTestInstance() {
        return new TsInfoExec(
            randomSource(),
            randomChild(0),
            randomFieldAttributes(1, 7, false),
            randomFieldAttributes(1, 7, false),
            randomFrom(TsInfoExec.Mode.values()),
            randomLimit()
        );
    }

    @Override
    protected TsInfoExec mutateInstance(TsInfoExec instance) throws IOException {
        PhysicalPlan child = instance.child();
        List<Attribute> outputAttrs = instance.outputAttrs();
        List<Attribute> intermediateAttributes = instance.intermediateAttributes();
        TsInfoExec.Mode mode = instance.mode();
        Integer limit = instance.limit();
        switch (between(0, 4)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> outputAttrs = randomValueOtherThan(outputAttrs, () -> randomFieldAttributes(1, 7, false));
            case 2 -> intermediateAttributes = randomValueOtherThan(intermediateAttributes, () -> randomFieldAttributes(1, 7, false));
            case 3 -> mode = randomValueOtherThan(mode, () -> randomFrom(TsInfoExec.Mode.values()));
            case 4 -> limit = randomValueOtherThan(limit, TsInfoExecSerializationTests::randomLimit);
            default -> throw new IllegalStateException("unexpected mutation");
        }
        return new TsInfoExec(instance.source(), child, outputAttrs, intermediateAttributes, mode, limit);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }

    public void testLimitOnTheWire() throws IOException {
        for (TransportVersion version : LIMIT_ON_THE_WIRE) {
            TsInfoExec instance = limitedInstance(version);
            assertEquals(version.toString(), instance.limit(), copyInstance(instance, version).limit());
        }
    }

    public void testLimitNotOnTheWire() throws IOException {
        for (TransportVersion version : LIMIT_NOT_ON_THE_WIRE) {
            TsInfoExec instance = limitedInstance(version);
            assertNull(version.toString(), copyInstance(instance, version).limit());
        }
    }

    private static TsInfoExec limitedInstance(TransportVersion version) {
        return new TsInfoExec(
            randomSource(),
            new ExchangeSourceExec(randomSource(), randomFieldAttributes(1, 5, false, version), randomBoolean()),
            randomFieldAttributes(1, 5, false, version),
            randomFieldAttributes(1, 5, false, version),
            randomFrom(TsInfoExec.Mode.values()),
            between(1, 100_000)
        );
    }

    private static Integer randomLimit() {
        return randomBoolean() ? null : between(1, 100_000);
    }
}
