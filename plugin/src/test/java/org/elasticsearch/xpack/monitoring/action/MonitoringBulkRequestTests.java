/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matcher;

import java.io.IOException;

import static org.elasticsearch.test.VersionUtils.randomVersion;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class MonitoringBulkRequestTests extends ESTestCase {

    private static final BytesArray SOURCE = new BytesArray("{\"key\" : \"value\"}");

    public void testValidateRequestNoDocs() {
        assertValidationErrors(new MonitoringBulkRequest(), hasItems("no monitoring documents added"));
    }

    public void testValidateRequestSingleDoc() {
        MonitoringBulkDoc doc = new MonitoringBulkDoc(null, null, null, null, null, null, null);

        assertValidationErrors(new MonitoringBulkRequest().add(doc), hasItems("monitored system id is missing for monitoring document [0]",
                "monitored system API version is missing for monitoring document [0]",
                "type is missing for monitoring document [0]",
                "source is missing for monitoring document [0]"));

        doc = new MonitoringBulkDoc("id", null, null, null, null, null, null);
        assertValidationErrors(new MonitoringBulkRequest().add(doc),
                hasItems("monitored system API version is missing for monitoring document [0]",
                "type is missing for monitoring document [0]",
                "source is missing for monitoring document [0]"));

        doc = new MonitoringBulkDoc("id", "version", null, null, null, null, null);
        assertValidationErrors(new MonitoringBulkRequest().add(doc), hasItems("type is missing for monitoring document [0]",
                "source is missing for monitoring document [0]"));

        doc.setType("type");
        assertValidationErrors(new MonitoringBulkRequest().add(doc), hasItems("source is missing for monitoring document [0]"));

        doc.setSource(SOURCE, XContentType.JSON);
        assertValidationErrors(new MonitoringBulkRequest().add(doc), nullValue());
    }


    public void testValidateRequestMultiDocs() {
        MonitoringBulkRequest request = new MonitoringBulkRequest();

        // Doc0 is complete
        MonitoringBulkDoc doc0 = new MonitoringBulkDoc(randomAlphaOfLength(2),
                randomAlphaOfLength(2), MonitoringIndex.TIMESTAMPED, randomAlphaOfLength(5),
                null, SOURCE, XContentType.JSON);
        request.add(doc0);

        // Doc1 has no type
        MonitoringBulkDoc doc1 = new MonitoringBulkDoc(randomAlphaOfLength(2),
                randomAlphaOfLength(2), MonitoringIndex.TIMESTAMPED, null,
                null, SOURCE, XContentType.JSON);
        request.add(doc1);

        // Doc2 has no source
        MonitoringBulkDoc doc2 = new MonitoringBulkDoc(randomAlphaOfLength(2),
                randomAlphaOfLength(2), MonitoringIndex.TIMESTAMPED, randomAlphaOfLength(5),
                null, BytesArray.EMPTY, XContentType.JSON);
        request.add(doc2);

        // Doc3 has no version
        MonitoringBulkDoc doc3 = new MonitoringBulkDoc(randomAlphaOfLength(2),
                null, MonitoringIndex.TIMESTAMPED, randomAlphaOfLength(5),
                null, SOURCE, XContentType.JSON);
        request.add(doc3);

        // Doc4 has no id
        MonitoringBulkDoc doc4 = new MonitoringBulkDoc(null,
                randomAlphaOfLength(2), MonitoringIndex.TIMESTAMPED, randomAlphaOfLength(5),
                null, SOURCE, XContentType.JSON);
        request.add(doc4);

        assertValidationErrors(request, hasItems("type is missing for monitoring document [1]",
                "source is missing for monitoring document [2]",
                "monitored system API version is missing for monitoring document [3]",
                "monitored system id is missing for monitoring document [4]"));

    }

    public void testAddSingleDoc() {
        MonitoringBulkRequest request = new MonitoringBulkRequest();
        final int nbDocs = randomIntBetween(1, 20);
        for (int i = 0; i < nbDocs; i++) {
            request.add(new MonitoringBulkDoc(String.valueOf(i), String.valueOf(i),
                    randomFrom(MonitoringIndex.values()), randomAlphaOfLength(5),
                    randomAlphaOfLength(5), SOURCE, XContentType.JSON));
        }
        assertThat(request.getDocs(), hasSize(nbDocs));
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/x-pack-elasticsearch/issues/1353")
    public void testAddMultipleDocs() throws Exception {
        final int nbDocs = randomIntBetween(3, 20);
        final String[] types = new String[nbDocs];
        final XContentType xContentType = XContentType.JSON;
        int dataCount = 0;
        int i;

        try (BytesStreamOutput content = new BytesStreamOutput()) {
            try (XContentBuilder builder = XContentFactory.contentBuilder(xContentType, content)) {
                for (i = 0; i < nbDocs; i++) {
                    builder.startObject().startObject("index");
                    if (rarely()) {
                        builder.field("_index", "");
                    }
                    if (randomBoolean()) {
                        types[i] = randomAlphaOfLength(5);
                        builder.field("_type", types[i]);
                    }
                    builder.endObject().endObject().flush();
                    content.write(xContentType.xContent().streamSeparator());
                    builder.startObject().field("foo").value(i).endObject().flush();
                    content.write(xContentType.xContent().streamSeparator());
                }
            }

            String defaultMonitoringId = randomBoolean() ? randomAlphaOfLength(2) : null;
            String defaultMonitoringVersion = randomBoolean() ? randomAlphaOfLength(3) : null;
            String defaultType = rarely() ? randomAlphaOfLength(4) : null;

            MonitoringBulkRequest request = new MonitoringBulkRequest();
            request.add(content.bytes(), defaultMonitoringId, defaultMonitoringVersion, defaultType, xContentType);
            assertThat(request.getDocs(), hasSize(nbDocs - dataCount));

            i = 0;

            for (final MonitoringBulkDoc doc : request.getDocs()) {
                final String expectedType = types[i] != null ? types[i] : defaultType;

                assertThat(doc.getMonitoringId(), equalTo(defaultMonitoringId));
                assertThat(doc.getMonitoringVersion(), equalTo(defaultMonitoringVersion));
                assertThat(doc.getType(), equalTo(expectedType));

                ++i;
            }
        }
    }

    public void testSerialization() throws IOException {
        MonitoringBulkRequest request = new MonitoringBulkRequest();

        int numDocs = iterations(10, 30);
        for (int i = 0; i < numDocs; i++) {
            request.add(MonitoringBulkDocTests.newRandomMonitoringBulkDoc());
        }

        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(randomVersion(random()));
        request.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        in.setVersion(out.getVersion());
        MonitoringBulkRequest request2 = new MonitoringBulkRequest();
        request2.readFrom(in);

        assertThat(in.available(), equalTo(0));
        assertThat(request2.docs.size(), equalTo(request.docs.size()));

        for (int i = 0; i < request2.docs.size(); i++) {
            MonitoringBulkDoc doc = request.docs.get(i);
            MonitoringBulkDoc doc2 = request2.docs.get(i);
            assertThat(doc2.getMonitoringId(), equalTo(doc.getMonitoringId()));
            assertThat(doc2.getMonitoringVersion(), equalTo(doc.getMonitoringVersion()));
            assertThat(doc2.getClusterUUID(), equalTo(doc.getClusterUUID()));
            assertThat(doc2.getTimestamp(), equalTo(doc.getTimestamp()));
            assertThat(doc2.getSourceNode(), equalTo(doc.getSourceNode()));
            assertThat(doc2.getIndex(), equalTo(doc.getIndex()));
            assertThat(doc2.getType(), equalTo(doc.getType()));
            assertThat(doc2.getId(), equalTo(doc.getId()));
            assertThat(doc2.getSource(), equalTo(doc.getSource()));
            assertThat(doc2.getXContentType(), equalTo(doc.getXContentType()));
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> void assertValidationErrors(MonitoringBulkRequest request, Matcher<? super T> matcher) {
        ActionRequestValidationException validation = request.validate();
        if (validation != null) {
            assertThat((T) validation.validationErrors(), matcher);
        } else {
            assertThat(null, matcher);
        }
    }
}
