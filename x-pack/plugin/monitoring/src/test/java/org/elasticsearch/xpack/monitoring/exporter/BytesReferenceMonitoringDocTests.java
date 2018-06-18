/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.test.RandomObjects;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.MonitoringTestUtils;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.elasticsearch.xpack.core.monitoring.MonitoredSystem.KIBANA;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests {@link BytesReferenceMonitoringDoc}
 */
public class BytesReferenceMonitoringDocTests extends BaseMonitoringDocTestCase<BytesReferenceMonitoringDoc> {

    private XContentType xContentType;
    private BytesReference source;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        xContentType = randomFrom(XContentType.values());
        source = RandomObjects.randomSource(random(), xContentType);
    }

    @Override
    protected BytesReferenceMonitoringDoc createMonitoringDoc(final String cluster, final long timestamp, final long intervalMillis,
                                                              final MonitoringDoc.Node node,
                                                              final MonitoredSystem system, final String type, final String id) {
        return new BytesReferenceMonitoringDoc(cluster, timestamp, intervalMillis, node, system, type, id, xContentType, source);
    }

    @Override
    protected void assertMonitoringDoc(final BytesReferenceMonitoringDoc document) {
        assertThat(document.getSystem(), equalTo(system));
        assertThat(document.getType(), equalTo(type));
        assertThat(document.getId(), equalTo(id));

        assertThat(document.getXContentType(), equalTo(xContentType));
        assertThat(document.getSource(), equalTo(source));
    }

    public void testConstructorMonitoredSystemMustNotBeNull() {
        expectThrows(NullPointerException.class,
                () -> new BytesReferenceMonitoringDoc(cluster, timestamp, interval, node, null, type, id, xContentType, source));
    }

    public void testConstructorTypeMustNotBeNull() {
        expectThrows(NullPointerException.class,
                () -> new BytesReferenceMonitoringDoc(cluster, timestamp, interval, node, system, null, id, xContentType, source));
    }

    public void testConstructorXContentTypeMustNotBeNull() {
        expectThrows(NullPointerException.class,
                () -> new BytesReferenceMonitoringDoc(cluster, timestamp, interval, node, system, type, id, null, source));
    }

    public void testConstructorSourceMustNotBeNull() {
        expectThrows(NullPointerException.class,
                () -> new BytesReferenceMonitoringDoc(cluster, timestamp, interval, node, system, type, id, xContentType, null));
    }

    @Override
    public void testToXContent() throws IOException {
        final XContentBuilder builder = XContentBuilder.builder(xContentType.xContent());
        builder.startObject();
        builder.field("field", "value");
        builder.endObject();

        final MonitoringDoc.Node node =
                new MonitoringDoc.Node("_uuid", "_host", "_addr", "_ip", "_name", 1504169190855L);
        final BytesReferenceMonitoringDoc document = new BytesReferenceMonitoringDoc("_cluster", 1502266739402L, 1506593717631L,
                node, KIBANA, "_type", "_id", xContentType, BytesReference.bytes(builder));

        final BytesReference xContent = XContentHelper.toXContent(document, XContentType.JSON, false);
        assertEquals("{"
                     + "\"cluster_uuid\":\"_cluster\","
                     + "\"timestamp\":\"2017-08-09T08:18:59.402Z\","
                     + "\"interval_ms\":1506593717631,"
                     + "\"type\":\"_type\","
                     + "\"source_node\":{"
                       + "\"uuid\":\"_uuid\","
                       + "\"host\":\"_host\","
                       + "\"transport_address\":\"_addr\","
                       + "\"ip\":\"_ip\","
                       + "\"name\":\"_name\","
                       + "\"timestamp\":\"2017-08-31T08:46:30.855Z\""
                     + "},"
                     + "\"_type\":{"
                        + "\"field\":\"value\""
                     + "}"
                    + "}", xContent.utf8ToString());
    }

    public void testEqualsAndHashcode() {
        final EqualsHashCodeTestUtils.CopyFunction<MonitoringDoc> copy = doc ->
                createMonitoringDoc(doc.getCluster(), doc.getTimestamp(), doc.getIntervalMillis(),
                                    doc.getNode(), doc.getSystem(), doc.getType(), doc.getId());

        final List<EqualsHashCodeTestUtils.MutateFunction<MonitoringDoc>> mutations = new ArrayList<>();
        mutations.add(doc -> {
            String cluster;
            do {
                cluster = UUIDs.randomBase64UUID();
            } while (cluster.equals(doc.getCluster()));
            return createMonitoringDoc(cluster, doc.getTimestamp(), doc.getIntervalMillis(),
                                       doc.getNode(), doc.getSystem(), doc.getType(), doc.getId());
        });
        mutations.add(doc -> {
            long timestamp;
            do {
                timestamp = randomNonNegativeLong();
            } while (timestamp == doc.getTimestamp());
            return createMonitoringDoc(doc.getCluster(), timestamp, doc.getIntervalMillis(),
                                       doc.getNode(), doc.getSystem(), doc.getType(), doc.getId());
        });
        mutations.add(doc -> {
            long intervaMillis;
            do {
                intervaMillis = randomNonNegativeLong();
            } while (intervaMillis == doc.getIntervalMillis());
            return createMonitoringDoc(doc.getCluster(), doc.getTimestamp(), intervaMillis,
                                       doc.getNode(), doc.getSystem(), doc.getType(), doc.getId());
        });
        mutations.add(doc -> {
            MonitoringDoc.Node node;
            do {
                node = MonitoringTestUtils.randomMonitoringNode(random());
            } while (node.equals(doc.getNode()));
            return createMonitoringDoc(doc.getCluster(), doc.getTimestamp(), doc.getIntervalMillis(),
                                       node, doc.getSystem(), doc.getType(), doc.getId());
        });
        mutations.add(doc -> {
            MonitoredSystem system;
            do {
                system = randomFrom(MonitoredSystem.values());
            } while (system == doc.getSystem());
            return createMonitoringDoc(doc.getCluster(), doc.getTimestamp(), doc.getIntervalMillis(),
                                       doc.getNode(), system, doc.getType(), doc.getId());
        });
        mutations.add(doc -> {
            String type;
            do {
                type = randomAlphaOfLength(5);
            } while (type.equals(doc.getType()));
            return createMonitoringDoc(doc.getCluster(), doc.getTimestamp(), doc.getIntervalMillis(),
                                       doc.getNode(), doc.getSystem(), type, doc.getId());
        });
        mutations.add(doc -> {
            String id;
            do {
                id = randomAlphaOfLength(10);
            } while (id.equals(doc.getId()));
            return createMonitoringDoc(doc.getCluster(), doc.getTimestamp(), doc.getIntervalMillis(),
                                       doc.getNode(), doc.getSystem(), doc.getType(), id);
        });

        checkEqualsAndHashCode(createMonitoringDoc(cluster, timestamp, interval, node, system, type, id), copy, randomFrom(mutations));
    }
}
