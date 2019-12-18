package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

public class SnapshotActionTests extends AbstractActionTestCase<SnapshotAction> {

    @Override
    public void testToSteps() {
        SnapshotAction action = createTestInstance();
        Step.StepKey nextStep = new Step.StepKey("", "", "");
        List<Step> steps = action.toSteps(null, "delete", nextStep);
        assertEquals(1, steps.size());
        Step step = steps.get(0);
        assertTrue(step instanceof WaitForSnapshotStep);
        assertEquals(nextStep, step.getNextStepKey());

        Step.StepKey key = step.getKey();
        assertEquals("delete", key.getPhase());
        assertEquals(SnapshotAction.NAME, key.getAction());
        assertEquals(WaitForSnapshotStep.NAME, key.getName());
    }

    @Override
    protected SnapshotAction doParseInstance(XContentParser parser) throws IOException {
        return SnapshotAction.parse(parser);
    }

    @Override
    protected SnapshotAction createTestInstance() {
        return new SnapshotAction("policy");
    }

    @Override
    protected Writeable.Reader<SnapshotAction> instanceReader() {
        return SnapshotAction::new;
    }
}
