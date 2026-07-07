/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.iplocation.api.IpLocationConsumer;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.lessThan;

public class IpLocationDownloadConsumersTests extends AbstractChunkedSerializingTestCase<IpLocationDownloadConsumers> {

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(new IngestGeoIpPlugin().getNamedWriteables());
    }

    @Override
    protected IpLocationDownloadConsumers doParseInstance(XContentParser parser) throws IOException {
        return IpLocationDownloadConsumers.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<IpLocationDownloadConsumers> instanceReader() {
        return IpLocationDownloadConsumers::new;
    }

    @Override
    protected IpLocationDownloadConsumers createTestInstance() {
        return new IpLocationDownloadConsumers(randomConsumers());
    }

    @Override
    protected IpLocationDownloadConsumers mutateInstance(IpLocationDownloadConsumers instance) {
        IpLocationConsumer[] allConsumers = IpLocationConsumer.values();
        int presentCount = 0;
        for (IpLocationConsumer c : allConsumers) {
            if (instance.contains(c)) {
                presentCount++;
            }
        }
        boolean canAdd = presentCount < allConsumers.length;
        boolean canRemove = presentCount > 0;
        boolean add = canAdd && (canRemove == false || randomBoolean());
        if (add) {
            IpLocationConsumer toAdd = randomValueOtherThanMany(instance::contains, () -> randomFrom(allConsumers));
            return instance.withConsumer(toAdd);
        } else {
            IpLocationConsumer toRemove = randomValueOtherThanMany(c -> instance.contains(c) == false, () -> randomFrom(allConsumers));
            return instance.withoutConsumer(toRemove);
        }
    }

    private Set<IpLocationConsumer> randomConsumers() {
        EnumSet<IpLocationConsumer> consumers = EnumSet.noneOf(IpLocationConsumer.class);
        for (IpLocationConsumer consumer : IpLocationConsumer.values()) {
            if (randomBoolean()) {
                consumers.add(consumer);
            }
        }
        return consumers;
    }

    public void testIsRelevantForNode_ingestConsumerMatchesIngestNodes() {
        assertTrue(
            "INGEST consumer is relevant for an ingest-only node",
            IpLocationDownloadConsumers.isRelevantForNode(IpLocationConsumer.INGEST, nodeWithRoles(DiscoveryNodeRole.INGEST_ROLE))
        );
        assertTrue(
            "INGEST consumer is relevant for a data+ingest node",
            IpLocationDownloadConsumers.isRelevantForNode(
                IpLocationConsumer.INGEST,
                nodeWithRoles(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.INGEST_ROLE)
            )
        );
        assertFalse(
            "INGEST consumer is not relevant for a data-only node",
            IpLocationDownloadConsumers.isRelevantForNode(IpLocationConsumer.INGEST, nodeWithRoles(DiscoveryNodeRole.DATA_ROLE))
        );
        assertFalse(
            "INGEST consumer is not relevant for a coordinating-only node",
            IpLocationDownloadConsumers.isRelevantForNode(IpLocationConsumer.INGEST, nodeWithRoles())
        );
    }

    public void testIsRelevantForNode_esqlConsumerMatchesAllNodes() {
        assertTrue(
            "ESQL consumer is relevant for a data-only node",
            IpLocationDownloadConsumers.isRelevantForNode(IpLocationConsumer.ESQL, nodeWithRoles(DiscoveryNodeRole.DATA_ROLE))
        );
        assertTrue(
            "ESQL consumer is relevant for a coordinating-only node (no roles)",
            IpLocationDownloadConsumers.isRelevantForNode(IpLocationConsumer.ESQL, nodeWithRoles())
        );
        assertTrue(
            "ESQL consumer is relevant for an ingest-only node",
            IpLocationDownloadConsumers.isRelevantForNode(IpLocationConsumer.ESQL, nodeWithRoles(DiscoveryNodeRole.INGEST_ROLE))
        );
        assertTrue(
            "ESQL consumer is relevant for a master-only node",
            IpLocationDownloadConsumers.isRelevantForNode(IpLocationConsumer.ESQL, nodeWithRoles(DiscoveryNodeRole.MASTER_ROLE))
        );
    }

    public void testParseIgnoresUnknownConsumerName() throws IOException {
        // Simulates a snapshot/cluster-state taken on a newer cluster that declares a consumer this node does not
        // know about. Parsing must succeed and drop the unknown value (production logs a WARN -- see fromXContent)
        // rather than fail metadata recovery.
        String json = "{\"consumers\":[\"INGEST\",\"FUTURE_CONSUMER_THAT_DOES_NOT_EXIST\"]}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            IpLocationDownloadConsumers parsed = IpLocationDownloadConsumers.fromXContent(parser);
            assertTrue("known INGEST consumer should survive", parsed.contains(IpLocationConsumer.INGEST));
            for (IpLocationConsumer consumer : IpLocationConsumer.values()) {
                if (consumer != IpLocationConsumer.INGEST) {
                    assertFalse(consumer.name() + " should not be present", parsed.contains(consumer));
                }
            }
        }
    }

    public void testDiffRoundTripAndApply() throws IOException {
        IpLocationDownloadConsumers empty = new IpLocationDownloadConsumers(EnumSet.noneOf(IpLocationConsumer.class));
        IpLocationDownloadConsumers ingest = new IpLocationDownloadConsumers(EnumSet.of(IpLocationConsumer.INGEST));
        IpLocationDownloadConsumers both = new IpLocationDownloadConsumers(EnumSet.allOf(IpLocationConsumer.class));

        // unchanged -> no-op diff; apply must yield an instance equal to before/after
        assertDiffRoundTrip(ingest, ingest, ingest);
        assertDiffRoundTrip(empty, empty, empty);
        // transitions
        assertDiffRoundTrip(empty, ingest, ingest);
        assertDiffRoundTrip(ingest, both, both);
        assertDiffRoundTrip(both, ingest, ingest);
        assertDiffRoundTrip(ingest, empty, empty);
    }

    public void testDiffNoOpIsSmallerThanFullSnapshot() throws IOException {
        IpLocationDownloadConsumers both = new IpLocationDownloadConsumers(EnumSet.allOf(IpLocationConsumer.class));
        IpLocationDownloadConsumers empty = new IpLocationDownloadConsumers(EnumSet.noneOf(IpLocationConsumer.class));

        BytesStreamOutput noOp = new BytesStreamOutput();
        both.diff(both).writeTo(noOp);

        BytesStreamOutput changed = new BytesStreamOutput();
        both.diff(empty).writeTo(changed);

        assertThat("no-op diff should be smaller on the wire than a full-snapshot diff", noOp.size(), lessThan(changed.size()));
    }

    private static void assertDiffRoundTrip(
        IpLocationDownloadConsumers before,
        IpLocationDownloadConsumers after,
        IpLocationDownloadConsumers expected
    ) throws IOException {
        Diff<Metadata.ProjectCustom> diff = after.diff(before);
        Diff<Metadata.ProjectCustom> deserialized = copyWriteable(
            diff,
            new NamedWriteableRegistry(List.of()),
            IpLocationDownloadConsumers.IpLocationDownloadConsumersDiff::new
        );
        assertEquals(expected, deserialized.apply(before));
    }

    private static DiscoveryNode nodeWithRoles(DiscoveryNodeRole... roles) {
        return DiscoveryNodeUtils.builder("_node_id").roles(Set.of(roles)).build();
    }
}
