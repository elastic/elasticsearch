package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

public class NodeDecisionWireSerializationTests extends AbstractWireSerializingTestCase<NodeDecision> {

    @Override
    protected Writeable.Reader<NodeDecision> instanceReader() {
        return NodeDecision::new;
    }

    @Override
    protected NodeDecision createTestInstance() {
        return new NodeDecision(
            new DiscoveryNode(randomAlphaOfLength(6), buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT),
            randomFrom(
                new Decision.Single(Decision.Type.NO, DiskThresholdDecider.NAME, "Unable to allocate on disk"),
                new Decision.Single(Decision.Type.YES, FilterAllocationDecider.NAME, "Filter allows allocation"),
                new Decision.Single(Decision.Type.THROTTLE, "throttle label", "Throttling the consumer"),
                new Decision.Multi().add(randomFrom(Decision.NO, Decision.YES, Decision.THROTTLE))
                    .add(new Decision.Single(Decision.Type.NO, "multi_no", "No multi decision"))
                    .add(new Decision.Single(Decision.Type.YES, "multi_yes", "Yes multi decision"))
            )
        );
    }
}
