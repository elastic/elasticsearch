/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

public class NodeDecisionWireSerializationTests extends AbstractWireSerializingTestCase<NodeDecision> {

    @Override
    protected NodeDecision mutateInstance(NodeDecision instance) throws IOException {
        if (randomBoolean()) {
            return new NodeDecision(
                new DiscoveryNode(randomAlphaOfLength(6), buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT),
                instance.decision()
            );
        } else if (randomBoolean()) {
            return new NodeDecision(instance.node(), randomFrom(Decision.NO, Decision.THROTTLE, Decision.YES));
        } else {
            return randomValueOtherThan(instance, this::createTestInstance);
        }
    }

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
