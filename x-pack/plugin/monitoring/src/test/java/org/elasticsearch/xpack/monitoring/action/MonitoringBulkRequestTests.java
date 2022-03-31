/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.action;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.RandomObjects;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringBulkDoc;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringBulkRequest;
import org.elasticsearch.xpack.monitoring.MonitoringTestUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;

/**
 * Tests {@link MonitoringBulkRequest}
 */
public class MonitoringBulkRequestTests extends ESTestCase {

    public void testValidateWithNoDocs() {
        ActionRequestValidationException validation = new MonitoringBulkRequest().validate();
        assertNotNull(validation);
        assertThat(validation.validationErrors(), hasItem("no monitoring documents added"));
    }

    public void testValidateWithEmptySource() throws IOException {
        final MonitoringBulkRequest request = new MonitoringBulkRequest();

        final int nbDocs = randomIntBetween(0, 5);
        for (int i = 0; i < nbDocs; i++) {
            request.add(randomMonitoringBulkDoc());
        }

        final int nbEmptyDocs = randomIntBetween(1, 20);
        for (int i = 0; i < nbEmptyDocs; i++) {
            request.add(MonitoringTestUtils.randomMonitoringBulkDoc(random(), randomXContentType(), BytesArray.EMPTY));
        }

        final ActionRequestValidationException validation = request.validate();
        assertNotNull(validation);

        final List<String> validationErrors = validation.validationErrors();
        for (int i = 0; i < nbEmptyDocs; i++) {
            assertThat(validationErrors, hasItem("source is missing for monitoring document [" + String.valueOf(nbDocs + i) + "]"));
        }
    }

    public void testAdd() throws IOException {
        final MonitoringBulkRequest request = new MonitoringBulkRequest();

        final int nbDocs = randomIntBetween(1, 20);
        for (int i = 0; i < nbDocs; i++) {
            request.add(randomMonitoringBulkDoc());
        }

        assertThat(request.getDocs(), hasSize(nbDocs));
    }

    public void testAddRequestContent() throws IOException {
        final XContentType xContentType = XContentType.JSON;

        final int nbDocs = randomIntBetween(1, 20);
        final String[] ids = new String[nbDocs];
        final BytesReference[] sources = new BytesReference[nbDocs];

        final BytesStreamOutput content = new BytesStreamOutput();
        try (XContentBuilder builder = XContentFactory.contentBuilder(xContentType, content)) {
            for (int i = 0; i < nbDocs; i++) {
                builder.startObject();
                {
                    builder.startObject("index");
                    {
                        if (rarely()) {
                            builder.field("_index", "");
                        }

                        builder.field("_type", "_doc");

                        if (randomBoolean()) {
                            ids[i] = randomAlphaOfLength(10);
                            builder.field("_id", ids[i]);
                        }
                    }
                    builder.endObject();
                }
                builder.endObject();

                builder.flush();
                content.write(xContentType.xContent().streamSeparator());

                sources[i] = RandomObjects.randomSource(random(), xContentType);
                BytesRef bytes = sources[i].toBytesRef();
                content.write(bytes.bytes, bytes.offset, bytes.length);

                content.write(xContentType.xContent().streamSeparator());
            }

            content.write(xContentType.xContent().streamSeparator());
        }

        final MonitoredSystem system = randomFrom(MonitoredSystem.values());
        final long timestamp = randomNonNegativeLong();
        final long interval = randomNonNegativeLong();

        final MonitoringBulkRequest bulkRequest = new MonitoringBulkRequest();
        bulkRequest.add(system, content.bytes(), xContentType, timestamp, interval);

        final Collection<MonitoringBulkDoc> bulkDocs = bulkRequest.getDocs();
        assertNotNull(bulkDocs);
        assertEquals(nbDocs, bulkDocs.size());

        int count = 0;
        for (final MonitoringBulkDoc bulkDoc : bulkDocs) {
            assertThat(bulkDoc.getSystem(), equalTo(system));
            assertThat(bulkDoc.getId(), equalTo(ids[count]));
            assertThat(bulkDoc.getTimestamp(), equalTo(timestamp));
            assertThat(bulkDoc.getIntervalMillis(), equalTo(interval));
            assertThat(bulkDoc.getSource(), equalTo(sources[count]));
            assertThat(bulkDoc.getXContentType(), equalTo(xContentType));
            ++count;
        }
    }

    public void testAddRequestContentWithEmptySource() throws IOException {
        final int nbDocs = randomIntBetween(0, 5);
        final int nbEmptyDocs = randomIntBetween(1, 10);
        final int totalDocs = nbDocs + nbEmptyDocs;

        final XContentType xContentType = XContentType.JSON;
        final byte separator = xContentType.xContent().streamSeparator();

        final BytesStreamOutput content = new BytesStreamOutput();
        try (XContentBuilder builder = XContentFactory.contentBuilder(xContentType, content)) {
            for (int i = 0; i < totalDocs; i++) {
                builder.startObject();
                {
                    builder.startObject("index");
                    {
                        builder.field("_index", "");
                        builder.field("_type", "_doc");
                        builder.field("_id", String.valueOf(i));
                    }
                    builder.endObject();
                }
                builder.endObject();

                builder.flush();
                content.write(separator);

                final BytesRef bytes;
                if (i < nbDocs) {
                    bytes = RandomObjects.randomSource(random(), xContentType).toBytesRef();
                } else {
                    bytes = BytesArray.EMPTY.toBytesRef();
                }
                content.write(bytes.bytes, bytes.offset, bytes.length);
                content.write(separator);
            }

            content.write(separator);
        }

        final MonitoringBulkRequest bulkRequest = new MonitoringBulkRequest();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> bulkRequest.add(randomFrom(MonitoredSystem.values()), content.bytes(), xContentType, 0L, 0L)
        );

        assertThat(e.getMessage(), containsString("source is missing for monitoring document [][_doc][" + nbDocs + "]"));
    }

    public void testAddRequestContentWithUnrecognizedIndexName() throws IOException {
        final String indexName = randomAlphaOfLength(10);

        final XContentType xContentType = XContentType.JSON;
        final byte separator = xContentType.xContent().streamSeparator();

        final BytesStreamOutput content = new BytesStreamOutput();
        try (XContentBuilder builder = XContentFactory.contentBuilder(xContentType, content)) {
            builder.startObject();
            {
                builder.startObject("index");
                {
                    builder.field("_index", indexName);
                }
                builder.endObject();
            }
            builder.endObject();

            builder.flush();
            content.write(separator);

            final BytesRef bytes = RandomObjects.randomSource(random(), xContentType).toBytesRef();
            content.write(bytes.bytes, bytes.offset, bytes.length);
            content.write(separator);

            content.write(separator);
        }

        final MonitoringBulkRequest bulkRequest = new MonitoringBulkRequest();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> bulkRequest.add(randomFrom(MonitoredSystem.values()), content.bytes(), xContentType, 0L, 0L)
        );

        assertThat(e.getMessage(), containsString("unrecognized index name [" + indexName + "]"));
    }

    public void testSerialization() throws IOException {
        final MonitoringBulkRequest originalRequest = new MonitoringBulkRequest();

        final int numDocs = iterations(10, 30);
        for (int i = 0; i < numDocs; i++) {
            originalRequest.add(randomMonitoringBulkDoc());
        }

        final BytesStreamOutput out = new BytesStreamOutput();
        originalRequest.writeTo(out);

        final StreamInput in = out.bytes().streamInput();
        in.setVersion(out.getVersion());

        final MonitoringBulkRequest deserializedRequest = new MonitoringBulkRequest(in);

        assertThat(in.available(), equalTo(0));

        final MonitoringBulkDoc[] originalBulkDocs = originalRequest.getDocs().toArray(new MonitoringBulkDoc[] {});
        final MonitoringBulkDoc[] deserializedBulkDocs = deserializedRequest.getDocs().toArray(new MonitoringBulkDoc[] {});

        assertArrayEquals(originalBulkDocs, deserializedBulkDocs);
    }

    /**
     * Return a {@link XContentType} supported by the Monitoring Bulk API (JSON or Smile)
     */
    private XContentType randomXContentType() {
        return randomFrom(XContentType.JSON, XContentType.SMILE);
    }

    private MonitoringBulkDoc randomMonitoringBulkDoc() throws IOException {
        return MonitoringTestUtils.randomMonitoringBulkDoc(random(), randomXContentType());
    }
}
