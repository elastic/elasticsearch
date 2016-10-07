/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.VersionUtils.randomVersion;
import static org.hamcrest.Matchers.equalTo;

public class MonitoringDocTests extends ESTestCase {

    public void testSerialization() throws IOException {
        int iterations = randomIntBetween(5, 50);
        for (int i = 0; i < iterations; i++) {
            MonitoringDoc monitoringDoc = new MonitoringDoc(randomAsciiOfLength(2), randomAsciiOfLength(2));
            if (frequently()) {
                monitoringDoc.setClusterUUID(randomAsciiOfLength(5));
            }
            if (randomBoolean()) {
                monitoringDoc.setTimestamp(System.currentTimeMillis());
            }

            boolean hasSourceNode = randomBoolean();
            if (hasSourceNode) {
                monitoringDoc.setSourceNode(newRandomSourceNode());
            }

            BytesStreamOutput output = new BytesStreamOutput();
            Version outputVersion = randomVersion(random());
            output.setVersion(outputVersion);
            monitoringDoc.writeTo(output);

            StreamInput streamInput = output.bytes().streamInput();
            streamInput.setVersion(randomVersion(random()));
            MonitoringDoc monitoringDoc2 = new MonitoringDoc(streamInput);

            assertThat(monitoringDoc2.getMonitoringId(), equalTo(monitoringDoc.getMonitoringId()));
            assertThat(monitoringDoc2.getMonitoringVersion(), equalTo(monitoringDoc.getMonitoringVersion()));
            assertThat(monitoringDoc2.getClusterUUID(), equalTo(monitoringDoc.getClusterUUID()));
            assertThat(monitoringDoc2.getTimestamp(), equalTo(monitoringDoc.getTimestamp()));
            assertThat(monitoringDoc2.getSourceNode(), equalTo(monitoringDoc.getSourceNode()));
            if (hasSourceNode) {
                assertThat(monitoringDoc2.getSourceNode().getUUID(), equalTo(monitoringDoc.getSourceNode().getUUID()));
                assertThat(monitoringDoc2.getSourceNode().getName(), equalTo(monitoringDoc.getSourceNode().getName()));
                assertThat(monitoringDoc2.getSourceNode().getIp(), equalTo(monitoringDoc.getSourceNode().getIp()));
                assertThat(monitoringDoc2.getSourceNode().getTransportAddress(),
                        equalTo(monitoringDoc.getSourceNode().getTransportAddress()));
                assertThat(monitoringDoc2.getSourceNode().getAttributes(), equalTo(monitoringDoc.getSourceNode().getAttributes()));
            }
        }
    }

    public void testSetSourceNode() {
        int iterations = randomIntBetween(5, 50);
        for (int i = 0; i < iterations; i++) {
            MonitoringDoc monitoringDoc = new MonitoringDoc(null, null);

            if (randomBoolean()) {
                DiscoveryNode discoveryNode = newRandomDiscoveryNode();
                monitoringDoc.setSourceNode(discoveryNode);

                assertThat(monitoringDoc.getSourceNode().getUUID(), equalTo(discoveryNode.getId()));
                assertThat(monitoringDoc.getSourceNode().getHost(), equalTo(discoveryNode.getHostName()));
                assertThat(monitoringDoc.getSourceNode().getTransportAddress(), equalTo(discoveryNode.getAddress().toString()));
                assertThat(monitoringDoc.getSourceNode().getIp(), equalTo(discoveryNode.getHostAddress()));
                assertThat(monitoringDoc.getSourceNode().getName(), equalTo(discoveryNode.getName()));
                assertThat(monitoringDoc.getSourceNode().getAttributes(), equalTo(discoveryNode.getAttributes()));
            } else {
                MonitoringDoc.Node node = newRandomSourceNode();
                monitoringDoc.setSourceNode(node);

                assertThat(monitoringDoc.getSourceNode().getUUID(), equalTo(node.getUUID()));
                assertThat(monitoringDoc.getSourceNode().getHost(), equalTo(node.getHost()));
                assertThat(monitoringDoc.getSourceNode().getTransportAddress(), equalTo(node.getTransportAddress()));
                assertThat(monitoringDoc.getSourceNode().getIp(), equalTo(node.getIp()));
                assertThat(monitoringDoc.getSourceNode().getName(), equalTo(node.getName()));
                assertThat(monitoringDoc.getSourceNode().getAttributes(), equalTo(node.getAttributes()));
            }
        }
    }

    private MonitoringDoc.Node newRandomSourceNode() {
        String uuid = null;
        String name = null;
        String ip = null;
        String transportAddress = null;
        String host = null;
        Map<String, String> attributes = null;

        if (frequently()) {
            uuid = randomAsciiOfLength(5);
            name = randomAsciiOfLength(5);
        }
        if (randomBoolean()) {
            ip = randomAsciiOfLength(5);
            transportAddress = randomAsciiOfLength(5);
            host = randomAsciiOfLength(3);
        }
        if (rarely()) {
            int nbAttributes = randomIntBetween(0, 5);
            attributes = new HashMap<>();
            for (int i = 0; i < nbAttributes; i++) {
                attributes.put("key#" + i, String.valueOf(i));
            }
        }
        return new MonitoringDoc.Node(uuid, host, transportAddress, ip, name, attributes);
    }

    private DiscoveryNode newRandomDiscoveryNode() {
        Map<String, String> attributes = new HashMap<>();
        if (rarely()) {
            int nbAttributes = randomIntBetween(0, 5);
            for (int i = 0; i < nbAttributes; i++) {
                attributes.put("key#" + i, String.valueOf(i));
            }
        }
        Set<DiscoveryNode.Role> roles = new HashSet<>(randomSubsetOf(Arrays.asList(DiscoveryNode.Role.values())));
        return new DiscoveryNode(randomAsciiOfLength(5), randomAsciiOfLength(3), buildNewFakeTransportAddress(),
                attributes, roles, randomVersion(random()));
    }
}
