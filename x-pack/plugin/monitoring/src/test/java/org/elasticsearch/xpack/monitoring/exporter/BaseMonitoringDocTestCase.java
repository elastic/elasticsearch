/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.exporter;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.MonitoringTestUtils;
import org.elasticsearch.xpack.monitoring.collector.shards.ShardMonitoringDoc;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Base tests of all {@link MonitoringDoc}
 */
public abstract class BaseMonitoringDocTestCase<T extends MonitoringDoc> extends ESTestCase {

    protected String cluster;
    protected long timestamp;
    protected long interval;
    protected MonitoringDoc.Node node;
    protected MonitoredSystem system;
    protected String type;
    protected String id;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        cluster = UUIDs.randomBase64UUID();
        timestamp = frequently() ? randomLongBetween(1, DateUtils.MAX_MILLIS_BEFORE_9999) : 0L;
        interval = randomNonNegativeLong();
        node = frequently() ? MonitoringTestUtils.randomMonitoringNode(random()) : null;
        system = randomFrom(MonitoredSystem.values());
        type = randomAlphaOfLength(5);
        id = randomBoolean() ? randomAlphaOfLength(10) : null;
    }

    /**
     * Creates the {@link MonitoringDoc} to test. Returned value must be deterministic,
     * ie multiple calls with the same parameters within the same test must return
     * identical objects.
     */
    protected abstract T createMonitoringDoc(String cluster,
                                             long timestamp,
                                             long interval,
                                             @Nullable MonitoringDoc.Node node,
                                             MonitoredSystem system,
                                             String type,
                                             @Nullable String id);

    /**
     * Assert that two {@link MonitoringDoc} are equal. By default, it
     * uses {@link MonitoringDoc#equals(Object)} and {@link MonitoringDoc#hashCode()} methods
     * and also checks XContent equality.
     */
    private void assertMonitoringDocEquals(T expected, T actual) throws IOException {
        assertEquals(expected, actual);
        assertEquals(expected.hashCode(), actual.hashCode());

        final boolean human = randomBoolean();
        final XContentType xContentType = randomFrom(XContentType.values());
        assertToXContentEquivalent(toXContent(expected, xContentType, human), toXContent(actual, xContentType, human), xContentType);
    }

    public final void testCreateMonitoringDoc() throws IOException {
        final int nbIterations = randomIntBetween(3, 20);
        for (int i = 0; i < nbIterations; i++) {
            final T document1 = createMonitoringDoc(cluster, timestamp, interval, node, system, type, id);
            final T document2 = createMonitoringDoc(cluster, timestamp, interval, node, system, type, id);

            assertNotSame(document1, document2);
            assertMonitoringDocEquals(document1, document2);
        }
    }

    public final void testConstructorClusterMustNotBeNull() {
        expectThrows(NullPointerException.class, () -> createMonitoringDoc(null, timestamp, interval, node, system, type, id));
    }

    public final void testConstructor() {
        final T document = createMonitoringDoc(cluster, timestamp, interval, node, system, type, id);

        assertThat(document.getCluster(), equalTo(cluster));
        assertThat(document.getTimestamp(), equalTo(timestamp));
        assertThat(document.getIntervalMillis(), equalTo(interval));
        assertThat(document.getNode(), equalTo(node));

        assertMonitoringDoc(document);
    }

    /**
     * Asserts that the specific fields of a {@link MonitoringDoc} have
     * the expected values.
     */
    protected abstract void assertMonitoringDoc(T document);

    public abstract void testToXContent() throws IOException;

    /**
     * Test that {@link MonitoringDoc} rendered using {@link ToXContent#toXContent(XContentBuilder, ToXContent.Params)}
     * contain a common set of fields.
     */
    public final void testToXContentContainsCommonFields() throws IOException {
        final XContentType xContentType = randomFrom(XContentType.values());
        final T document = createMonitoringDoc(cluster, timestamp, interval, node, system, type, id);

        final BytesReference bytes = XContentHelper.toXContent(document, xContentType, false);
        try (XContentParser parser = xContentType.xContent()
                .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, bytes.streamInput())) {
            final Map<String, ?> map = parser.map();

            assertThat(map.get("cluster_uuid"), equalTo(cluster));
            assertThat(map.get("timestamp"), equalTo(MonitoringDoc.toUTC(timestamp)));
            assertThat(map.get("interval_ms"), equalTo(document.getIntervalMillis()));
            assertThat(map.get("type"), equalTo(document.getType()));

            if (document.getType().equals(ShardMonitoringDoc.TYPE)) {
                assertThat(map.get("shard"), notNullValue());
            } else {
                assertThat(map.get(document.getType()), notNullValue());
            }

            @SuppressWarnings("unchecked")
            final Map<String, ?> sourceNode = (Map<String, ?>) map.get("source_node");
            if (node == null) {
                assertThat(sourceNode, nullValue());
            } else {
                assertThat(sourceNode.get("uuid"), equalTo(node.getUUID()));
                assertThat(sourceNode.get("transport_address"), equalTo(node.getTransportAddress()));
                assertThat(sourceNode.get("ip"), equalTo(node.getIp()));
                assertThat(sourceNode.get("host"), equalTo(node.getHost()));
                assertThat(sourceNode.get("name"), equalTo(node.getName()));
                assertThat(sourceNode.get("timestamp"), equalTo(MonitoringDoc.toUTC(node.getTimestamp())));
            }
        }
    }

    public void testMonitoringNodeConstructor() {
        final String id = randomAlphaOfLength(5);
        final String name = randomAlphaOfLengthBetween(3, 10);
        final TransportAddress fakeTransportAddress = buildNewFakeTransportAddress();
        final String host = fakeTransportAddress.address().getHostString();
        final String transportAddress = fakeTransportAddress.toString();
        final String ip = fakeTransportAddress.getAddress();
        final long timestamp = randomNonNegativeLong();

        final MonitoringDoc.Node node = new MonitoringDoc.Node(id, host, transportAddress, ip, name, timestamp);

        assertThat(node.getUUID(), equalTo(id));
        assertThat(node.getHost(), equalTo(host));
        assertThat(node.getTransportAddress(), equalTo(transportAddress));
        assertThat(node.getIp(), equalTo(ip));
        assertThat(node.getName(), equalTo(name));
        assertThat(node.getTimestamp(), equalTo(timestamp));
    }

    public void testMonitoringNodeToXContent() throws IOException {
        final MonitoringDoc.Node node = new MonitoringDoc.Node("_uuid", "_host", "_addr", "_ip", "_name", 1504169190855L);

        final BytesReference xContent = XContentHelper.toXContent(node, XContentType.JSON, randomBoolean());
        assertEquals("{"
                    + "\"uuid\":\"_uuid\","
                    + "\"host\":\"_host\","
                    + "\"transport_address\":\"_addr\","
                    + "\"ip\":\"_ip\","
                    + "\"name\":\"_name\","
                    + "\"timestamp\":\"2017-08-31T08:46:30.855Z\""
                + "}" , xContent.utf8ToString());
    }

    public void testMonitoringNodeEqualsAndHashcode() {
        final EqualsHashCodeTestUtils.CopyFunction<MonitoringDoc.Node> copy = node -> new MonitoringDoc.Node(node.getUUID(), node.getHost(),
                node.getTransportAddress(), node.getIp(), node.getName(), node.getTimestamp());

        final List<EqualsHashCodeTestUtils.MutateFunction<MonitoringDoc.Node>> mutations = new ArrayList<>();
        mutations.add(n -> {
            String id;
            do {
                id = UUIDs.randomBase64UUID();
            } while (id.equals(n.getUUID()));
            return new MonitoringDoc.Node(id, n.getHost(), n.getTransportAddress(), n.getIp(), n.getName(), n.getTimestamp());
        });
        mutations.add(n -> {
            String host;
            do {
                host = randomAlphaOfLength(10);
            } while (host.equals(n.getHost()));
            return new MonitoringDoc.Node(n.getUUID(), host, n.getTransportAddress(), n.getIp(), n.getName(), n.getTimestamp());
        });
        mutations.add(n -> {
            String transportAddress;
            do {
                transportAddress = randomAlphaOfLength(10);
            } while (transportAddress.equals(n.getTransportAddress()));
            return new MonitoringDoc.Node(n.getUUID(), n.getHost(), transportAddress, n.getIp(), n.getName(), n.getTimestamp());
        });
        mutations.add(n -> {
            String ip;
            do {
                ip = randomAlphaOfLength(10);
            } while (ip.equals(n.getIp()));
            return new MonitoringDoc.Node(n.getUUID(), n.getHost(), n.getTransportAddress(), ip, n.getName(), n.getTimestamp());
        });
        mutations.add(n -> {
            String name;
            do {
                name = randomAlphaOfLengthBetween(3, 10);
            } while (name.equals(n.getName()));
            return new MonitoringDoc.Node(n.getUUID(), n.getHost(), n.getTransportAddress(), n.getIp(), name, n.getTimestamp());
        });
        mutations.add(n -> {
            long timestamp;
            do {
                timestamp = randomBoolean() ? randomNonNegativeLong() : 0L;
            } while (timestamp == n.getTimestamp());
            return new MonitoringDoc.Node(n.getUUID(), n.getHost(), n.getTransportAddress(), n.getIp(), n.getName(), timestamp);
        });

        final MonitoringDoc.Node sourceNode = MonitoringTestUtils.randomMonitoringNode(random());

        checkEqualsAndHashCode(sourceNode, copy, randomFrom(mutations));
    }

    public void testMonitoringNodeSerialization() throws IOException {
        final NamedWriteableRegistry registry = new NamedWriteableRegistry(emptyList());

        final MonitoringDoc.Node original = MonitoringTestUtils.randomMonitoringNode(random());
        final MonitoringDoc.Node deserialized = copyWriteable(original, registry, MonitoringDoc.Node::new);

        assertEquals(deserialized, original);
        assertEquals(deserialized.hashCode(), original.hashCode());
        assertNotSame(deserialized, original);
    }
}
