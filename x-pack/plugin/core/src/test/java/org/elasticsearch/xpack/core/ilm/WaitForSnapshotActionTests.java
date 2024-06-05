/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

public class WaitForSnapshotActionTests extends AbstractActionTestCase<WaitForSnapshotAction> {

    @Override
    public void testToSteps() {
        WaitForSnapshotAction action = createTestInstance();
        Step.StepKey nextStep = new Step.StepKey("", "", "");
        List<Step> steps = action.toSteps(null, "delete", nextStep);
        assertEquals(1, steps.size());
        Step step = steps.get(0);
        assertTrue(step instanceof WaitForSnapshotStep);
        assertEquals(nextStep, step.getNextStepKey());

        Step.StepKey key = step.getKey();
        assertEquals("delete", key.phase());
        assertEquals(WaitForSnapshotAction.NAME, key.action());
        assertEquals(WaitForSnapshotStep.NAME, key.name());
    }

    @Override
    protected WaitForSnapshotAction doParseInstance(XContentParser parser) throws IOException {
        return WaitForSnapshotAction.parse(parser);
    }

    @Override
    protected WaitForSnapshotAction createTestInstance() {
        return randomInstance();
    }

    @Override
    protected Writeable.Reader<WaitForSnapshotAction> instanceReader() {
        return WaitForSnapshotAction::new;
    }

    @Override
    protected WaitForSnapshotAction mutateInstance(WaitForSnapshotAction instance) {
        return randomInstance();
    }

    static WaitForSnapshotAction randomInstance() {
        return new WaitForSnapshotAction(randomAlphaOfLengthBetween(5, 10));
    }

}
