/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.autoscaling.indexing;

import co.elastic.elasticsearch.stateless.autoscaling.MetricQuality;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class NodeIngestLoadSnapshotTests extends AbstractWireSerializingTestCase<NodeIngestLoadSnapshot> {

    @Override
    protected Writeable.Reader<NodeIngestLoadSnapshot> instanceReader() {
        return NodeIngestLoadSnapshot::new;
    }

    @Override
    protected NodeIngestLoadSnapshot createTestInstance() {
        return new NodeIngestLoadSnapshot(
            randomAlphaOfLength(12),
            randomAlphaOfLength(12),
            randomDoubleBetween(0, 128, true),
            randomFrom(MetricQuality.values())
        );
    }

    @Override
    protected NodeIngestLoadSnapshot mutateInstance(NodeIngestLoadSnapshot instance) throws IOException {
        return switch (between(0, 3)) {
            case 0 -> new NodeIngestLoadSnapshot(randomAlphaOfLength(14), instance.nodeName(), instance.load(), instance.metricQuality());
            case 1 -> new NodeIngestLoadSnapshot(instance.nodeId(), randomAlphaOfLength(14), instance.load(), instance.metricQuality());
            case 2 -> new NodeIngestLoadSnapshot(
                instance.nodeId(),
                instance.nodeName(),
                randomDoubleBetween(256, 512, true),
                instance.metricQuality()
            );
            case 3 -> new NodeIngestLoadSnapshot(
                randomValueOtherThan(instance.nodeId(), ESTestCase::randomIdentifier),
                instance.nodeName(),
                instance.load(),
                randomValueOtherThan(instance.metricQuality(), () -> randomFrom(MetricQuality.values()))
            );
            default -> throw new IllegalStateException("Unexpected value");
        };
    }
}
