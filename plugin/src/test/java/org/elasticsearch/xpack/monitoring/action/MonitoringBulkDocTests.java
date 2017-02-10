/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.action;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;

import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.test.VersionUtils.randomVersion;
import static org.hamcrest.Matchers.equalTo;

public class MonitoringBulkDocTests extends ESTestCase {

    public void testSerialization() throws IOException {
        int iterations = randomIntBetween(5, 50);
        for (int i = 0; i < iterations; i++) {
            MonitoringBulkDoc doc = newRandomMonitoringBulkDoc();

            boolean hasSourceNode = randomBoolean();
            if (hasSourceNode) {
                doc.setSourceNode(newRandomSourceNode());
            }

            BytesStreamOutput output = new BytesStreamOutput();
            Version outputVersion = randomVersion(random());
            output.setVersion(outputVersion);
            doc.writeTo(output);

            StreamInput streamInput = output.bytes().streamInput();
            streamInput.setVersion(outputVersion);
            MonitoringBulkDoc doc2 = new MonitoringBulkDoc(streamInput);

            assertThat(doc2.getMonitoringId(), equalTo(doc.getMonitoringId()));
            assertThat(doc2.getMonitoringVersion(), equalTo(doc.getMonitoringVersion()));
            assertThat(doc2.getClusterUUID(), equalTo(doc.getClusterUUID()));
            assertThat(doc2.getTimestamp(), equalTo(doc.getTimestamp()));
            assertThat(doc2.getSourceNode(), equalTo(doc.getSourceNode()));
            assertThat(doc2.getIndex(), equalTo(doc.getIndex()));
            assertThat(doc2.getType(), equalTo(doc.getType()));
            assertThat(doc2.getId(), equalTo(doc.getId()));
            assertThat(doc2.getXContentType(), equalTo(doc.getXContentType()));
            if (doc.getSource() == null) {
                assertThat(doc2.getSource(), equalTo(BytesArray.EMPTY));
            } else {
                assertThat(doc2.getSource(), equalTo(doc.getSource()));
            }
        }
    }

    public void testSerializationBwc() throws IOException {
        final byte[] data = Base64.getDecoder().decode("AQNtSWQBBTUuMS4yAAAAAQEEdHlwZQECaWQNeyJmb28iOiJiYXIifQAAAAAAAAAA");
        final Version version = randomFrom(Version.V_5_0_0, Version.V_5_0_1, Version.V_5_0_2,
                Version.V_5_0_3_UNRELEASED, Version.V_5_1_1_UNRELEASED, Version.V_5_1_2_UNRELEASED, Version.V_5_2_0_UNRELEASED);
        try (StreamInput in = StreamInput.wrap(data)) {
            in.setVersion(version);
            MonitoringBulkDoc bulkDoc = new MonitoringBulkDoc(in);
            assertEquals(XContentType.JSON, bulkDoc.getXContentType());
            assertEquals("mId", bulkDoc.getMonitoringId());
            assertEquals("5.1.2", bulkDoc.getMonitoringVersion());
            assertEquals(MonitoringIndex.TIMESTAMPED, bulkDoc.getIndex());
            assertEquals("{\"foo\":\"bar\"}", bulkDoc.getSource().utf8ToString());
            assertEquals("type", bulkDoc.getType());
            assertEquals("id", bulkDoc.getId());

            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.setVersion(version);
                bulkDoc.writeTo(out);
                assertArrayEquals(data, out.bytes().toBytesRef().bytes);
            }
        }
    }

    private MonitoringBulkDoc newRandomMonitoringBulkDoc() {
        MonitoringBulkDoc doc = new MonitoringBulkDoc(randomAsciiOfLength(2), randomAsciiOfLength(2));
        if (frequently()) {
            doc.setClusterUUID(randomAsciiOfLength(5));
            doc.setType(randomAsciiOfLength(5));
        }
        if (randomBoolean()) {
            doc.setTimestamp(System.currentTimeMillis());
            doc.setSource(new BytesArray("{\"key\" : \"value\"}"), XContentType.JSON);
        }
        if (rarely()) {
            doc.setIndex(MonitoringIndex.DATA);
            doc.setId(randomAsciiOfLength(2));
        }
        return doc;
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
}
