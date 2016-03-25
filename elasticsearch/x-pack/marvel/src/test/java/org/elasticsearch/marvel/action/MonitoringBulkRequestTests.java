/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;

import java.io.IOException;

import static org.elasticsearch.test.VersionUtils.randomVersion;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

public class MonitoringBulkRequestTests extends ESTestCase {

    private static final BytesArray SOURCE = new BytesArray("{\"key\" : \"value\"}");

    public void testValidateRequestNoDocs() {
        assertValidationErrors(new MonitoringBulkRequest(), hasItems("no monitoring documents added"));
    }

    public void testValidateRequestSingleDoc() {
        MonitoringBulkDoc doc = new MonitoringBulkDoc(null, null);

        assertValidationErrors(new MonitoringBulkRequest().add(doc), hasItems("monitored system id is missing for monitoring document [0]",
                "monitored system version is missing for monitoring document [0]",
                "type is missing for monitoring document [0]",
                "source is missing for monitoring document [0]"));

        doc = new MonitoringBulkDoc("id", null);
        assertValidationErrors(new MonitoringBulkRequest().add(doc),
                hasItems("monitored system version is missing for monitoring document [0]",
                "type is missing for monitoring document [0]",
                "source is missing for monitoring document [0]"));

        doc = new MonitoringBulkDoc("id", "version");
        assertValidationErrors(new MonitoringBulkRequest().add(doc), hasItems("type is missing for monitoring document [0]",
                "source is missing for monitoring document [0]"));

        doc.setType("type");
        assertValidationErrors(new MonitoringBulkRequest().add(doc), hasItems("source is missing for monitoring document [0]"));

        doc.setSource(SOURCE);
        assertValidationErrors(new MonitoringBulkRequest().add(doc), nullValue());
    }


    public void testValidateRequestMultiDocs() {
        MonitoringBulkRequest request = new MonitoringBulkRequest();

        // Doc0 is complete
        MonitoringBulkDoc doc0 = new MonitoringBulkDoc(randomAsciiOfLength(2), randomAsciiOfLength(2));
        doc0.setType(randomAsciiOfLength(5));
        doc0.setSource(SOURCE);
        request.add(doc0);

        // Doc1 has no type
        MonitoringBulkDoc doc1 = new MonitoringBulkDoc(randomAsciiOfLength(2), randomAsciiOfLength(2));
        doc1.setSource(SOURCE);
        request.add(doc1);

        // Doc2 has no source
        MonitoringBulkDoc doc2 = new MonitoringBulkDoc(randomAsciiOfLength(2), randomAsciiOfLength(2));
        doc2.setType(randomAsciiOfLength(5));
        doc2.setSource(BytesArray.EMPTY);
        request.add(doc2);

        // Doc3 has no version
        MonitoringBulkDoc doc3 = new MonitoringBulkDoc(randomAsciiOfLength(2), null);
        doc3.setType(randomAsciiOfLength(5));
        doc3.setSource(SOURCE);
        request.add(doc3);

        // Doc4 has no id
        MonitoringBulkDoc doc4 = new MonitoringBulkDoc(null, randomAsciiOfLength(2));
        doc4.setType(randomAsciiOfLength(5));
        doc4.setSource(SOURCE);
        request.add(doc4);

        assertValidationErrors(request, hasItems("type is missing for monitoring document [1]",
                "source is missing for monitoring document [2]",
                "monitored system version is missing for monitoring document [3]",
                "monitored system id is missing for monitoring document [4]"));

    }

    public void testAddSingleDoc() {
        MonitoringBulkRequest request = new MonitoringBulkRequest();
        final int nbDocs = randomIntBetween(1, 20);
        for (int i = 0; i < nbDocs; i++) {
            request.add(new MonitoringBulkDoc(String.valueOf(i), String.valueOf(i)));
        }
        assertThat(request.getDocs(), hasSize(nbDocs));
    }

    public void testAddMultipleDocs() throws Exception {
        final int nbDocs = randomIntBetween(3, 20);
        final XContentType xContentType = XContentType.JSON;

        try (BytesStreamOutput content = new BytesStreamOutput()) {
            try (XContentBuilder builder = XContentFactory.contentBuilder(xContentType, content)) {
                for (int i = 0; i < nbDocs; i++) {
                    builder.startObject().startObject("index").endObject().endObject().flush();
                    content.write(xContentType.xContent().streamSeparator());
                    builder.startObject().field("foo").value(i).endObject().flush();
                    content.write(xContentType.xContent().streamSeparator());
                }
            }

            String defaultMonitoringId = randomBoolean() ? randomAsciiOfLength(2) : null;
            String defaultMonitoringVersion = randomBoolean() ? randomAsciiOfLength(3) : null;
            String defaultIndex = randomBoolean() ? randomAsciiOfLength(5) : null;
            String defaultType = randomBoolean() ? randomAsciiOfLength(4) : null;

            MonitoringBulkRequest request = new MonitoringBulkRequest();
            request.add(content.bytes(), defaultMonitoringId, defaultMonitoringVersion, defaultIndex, defaultType);
            assertThat(request.getDocs(), hasSize(nbDocs));

            for (MonitoringBulkDoc doc : request.getDocs()) {
                assertThat(doc.getMonitoringId(), equalTo(defaultMonitoringId));
                assertThat(doc.getMonitoringVersion(), equalTo(defaultMonitoringVersion));
                assertThat(doc.getIndex(), equalTo(defaultIndex));
                assertThat(doc.getType(), equalTo(defaultType));
            }
        }
    }

    public void testSerialization() throws IOException {
        MonitoringBulkRequest request = new MonitoringBulkRequest();

        int numDocs = iterations(10, 30);
        for (int i = 0; i < numDocs; i++) {
            MonitoringBulkDoc doc = new MonitoringBulkDoc(randomAsciiOfLength(2), randomVersion(random()).toString());
            doc.setType(randomFrom("type1", "type2", "type3"));
            doc.setSource(SOURCE);
            if (randomBoolean()) {
                doc.setIndex("index");
            }
            if (randomBoolean()) {
                doc.setId(randomAsciiOfLength(3));
            }
            if (rarely()) {
                doc.setClusterUUID(randomAsciiOfLength(5));
            }
            request.add(doc);
        }

        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(randomVersion(random()));
        request.writeTo(out);

        StreamInput in = StreamInput.wrap(out.bytes());
        in.setVersion(out.getVersion());
        MonitoringBulkRequest request2 = new MonitoringBulkRequest();
        request2.readFrom(in);

        assertThat(request2.docs.size(), CoreMatchers.equalTo(request.docs.size()));
        for (int i = 0; i < request2.docs.size(); i++) {
            MonitoringBulkDoc doc = request.docs.get(i);
            MonitoringBulkDoc doc2 = request2.docs.get(i);
            assertThat(doc2.getMonitoringId(), equalTo(doc.getMonitoringId()));
            assertThat(doc2.getMonitoringVersion(), equalTo(doc.getMonitoringVersion()));
            assertThat(doc2.getClusterUUID(), equalTo(doc.getClusterUUID()));
            assertThat(doc2.getIndex(), equalTo(doc.getIndex()));
            assertThat(doc2.getType(), equalTo(doc.getType()));
            assertThat(doc2.getId(), equalTo(doc.getId()));
            assertThat(doc2.getSource(), equalTo(doc.getSource()));
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
