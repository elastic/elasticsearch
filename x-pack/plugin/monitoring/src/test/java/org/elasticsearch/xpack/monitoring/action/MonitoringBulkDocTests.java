/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.action;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.test.RandomObjects;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringBulkDoc;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.elasticsearch.xpack.monitoring.MonitoringTestUtils.randomMonitoringBulkDoc;
import static org.hamcrest.Matchers.equalTo;

public class MonitoringBulkDocTests extends ESTestCase {

    private MonitoredSystem system;
    private String type;
    private String id;
    private long timestamp;
    private long interval;
    private BytesReference source;
    private XContentType xContentType;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        system = randomFrom(MonitoredSystem.values());
        type = randomAlphaOfLength(5);
        id = randomBoolean() ? randomAlphaOfLength(10) : null;
        timestamp = randomNonNegativeLong();
        interval = randomNonNegativeLong();
        xContentType = randomFrom(XContentType.values());
        source = RandomObjects.randomSource(random(), xContentType);
    }

    public void testConstructorMonitoredSystemMustNotBeNull() {
        expectThrows(NullPointerException.class, () -> new MonitoringBulkDoc(null, type, id, timestamp, interval, source, xContentType));
    }

    public void testConstructorTypeMustNotBeNull() {
        expectThrows(NullPointerException.class, () -> new MonitoringBulkDoc(system, null, id, timestamp, interval, source, xContentType));
    }

    public void testConstructorSourceMustNotBeNull() {
        expectThrows(NullPointerException.class, () -> new MonitoringBulkDoc(system, type, id, timestamp, interval, null, xContentType));
    }

    public void testConstructorXContentTypeMustNotBeNull() {
        expectThrows(NullPointerException.class, () -> new MonitoringBulkDoc(system, type, id, timestamp, interval, source, null));
    }

    public void testConstructor() {
        final MonitoringBulkDoc document = new MonitoringBulkDoc(system, type, id, timestamp, interval, source, xContentType);

        assertThat(document.getSystem(), equalTo(system));
        assertThat(document.getType(), equalTo(type));
        assertThat(document.getId(), equalTo(id));
        assertThat(document.getTimestamp(), equalTo(timestamp));
        assertThat(document.getIntervalMillis(), equalTo(interval));
        assertThat(document.getSource(), equalTo(source));
        assertThat(document.getXContentType(), equalTo(xContentType));
    }

    public void testEqualsAndHashcode() {
        final EqualsHashCodeTestUtils.CopyFunction<MonitoringBulkDoc> copy =
                doc -> new MonitoringBulkDoc(doc.getSystem(), doc.getType(), doc.getId(), doc.getTimestamp(), doc.getIntervalMillis(),
                                             doc.getSource(), doc.getXContentType());

        final List<EqualsHashCodeTestUtils.MutateFunction<MonitoringBulkDoc>> mutations = new ArrayList<>();
        mutations.add(doc -> {
            MonitoredSystem system;
            do {
                system = randomFrom(MonitoredSystem.values());
            } while (system == doc.getSystem());
            return new MonitoringBulkDoc(system, doc.getType(), doc.getId(), doc.getTimestamp(), doc.getIntervalMillis(),
                                         doc.getSource(),  doc.getXContentType());
        });
        mutations.add(doc -> {
            String type;
            do {
                type = randomAlphaOfLength(5);
            } while (type.equals(doc.getType()));
            return new MonitoringBulkDoc(doc.getSystem(), type, doc.getId(), doc.getTimestamp(), doc.getIntervalMillis(),
                                         doc.getSource(), doc.getXContentType());
        });
        mutations.add(doc -> {
            String id;
            do {
                id = randomAlphaOfLength(10);
            } while (id.equals(doc.getId()));
            return new MonitoringBulkDoc(doc.getSystem(), doc.getType(), id, doc.getTimestamp(), doc.getIntervalMillis(),
                                         doc.getSource(), doc.getXContentType());
        });
        mutations.add(doc -> {
            long timestamp;
            do {
                timestamp = randomNonNegativeLong();
            } while (timestamp == doc.getTimestamp());
            return new MonitoringBulkDoc(doc.getSystem(), doc.getType(), doc.getId(), timestamp, doc.getIntervalMillis(),
                                         doc.getSource(), doc.getXContentType());
        });
        mutations.add(doc -> {
            long interval;
            do {
                interval = randomNonNegativeLong();
            } while (interval == doc.getIntervalMillis());
            return new MonitoringBulkDoc(doc.getSystem(), doc.getType(), doc.getId(), doc.getTimestamp(), interval,
                                         doc.getSource(), doc.getXContentType());
        });
        mutations.add(doc -> {
            final BytesReference source = RandomObjects.randomSource(random(), doc.getXContentType());
            return new MonitoringBulkDoc(doc.getSystem(), doc.getType(), doc.getId(), doc.getTimestamp(), doc.getIntervalMillis(),
                                         source, doc.getXContentType());
        });
        mutations.add(doc -> {
            XContentType xContentType;
            do {
                xContentType = randomFrom(XContentType.values());
            } while (xContentType == doc.getXContentType());
            return new MonitoringBulkDoc(doc.getSystem(), doc.getType(), doc.getId(), doc.getTimestamp(), doc.getIntervalMillis(),
                                         doc.getSource(), xContentType);
        });

        final MonitoringBulkDoc document = new MonitoringBulkDoc(system, type, id, timestamp, interval, source, xContentType);
        checkEqualsAndHashCode(document, copy, randomFrom(mutations));
    }

    public void testSerialization() throws IOException {
        final NamedWriteableRegistry registry = new NamedWriteableRegistry(emptyList());

        final int iterations = randomIntBetween(5, 50);
        for (int i = 0; i < iterations; i++) {
            final MonitoringBulkDoc original = randomMonitoringBulkDoc(random());
            final MonitoringBulkDoc deserialized = copyWriteable(original, registry, MonitoringBulkDoc::new);

            assertEquals(original, deserialized);
            assertEquals(original.hashCode(), deserialized.hashCode());
            assertNotSame(original, deserialized);
        }
    }

    /**
     *  Test that we allow strings to be "" because Logstash 5.2 - 5.3 would submit empty _id values for time-based documents
     */
    public void testEmptyIdBecomesNull() {
        final String id = randomFrom("", null, randomAlphaOfLength(5));
        final MonitoringBulkDoc doc = new MonitoringBulkDoc(MonitoredSystem.ES, "_type", id, 1L, 2L, BytesArray.EMPTY, XContentType.JSON);

        if (Strings.isNullOrEmpty(id)) {
            assertNull(doc.getId());
        } else {
            assertSame(id, doc.getId());
        }
    }
}
