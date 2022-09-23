package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

import static org.elasticsearch.xpack.autoscaling.storage.NodeDecisionTestUtils.randomNodeDecision;

public class NodeDecisionsWireSerializationTests extends AbstractWireSerializingTestCase<NodeDecisions> {

    @Override
    protected NodeDecisions mutateInstance(NodeDecisions instance) throws IOException {
        if (randomBoolean()) {
            return new NodeDecisions(randomList(8, () -> NodeDecisionTestUtils.randomNodeDecision()), instance.canRemainDecisions());
        } else if (randomBoolean()) {
            return new NodeDecisions(instance.canAllocateDecisions(), randomList(8, () -> NodeDecisionTestUtils.randomNodeDecision()));
        } else {
            return randomValueOtherThan(instance, this::createTestInstance);
        }
    }

    @Override
    protected Writeable.Reader<NodeDecisions> instanceReader() {
        return NodeDecisions::new;
    }

    @Override
    protected NodeDecisions createTestInstance() {
        return new NodeDecisions(randomList(8, NodeDecisionTestUtils::randomNodeDecision), randomList(8, () -> randomNodeDecision()));
    }

}
