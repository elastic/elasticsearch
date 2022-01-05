/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.exporter;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.emptySet;
import static org.elasticsearch.xpack.monitoring.exporter.FilteredMonitoringDoc.COMMON_XCONTENT_FILTERS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

/**
 * Tests {@link FilteredMonitoringDoc}
 */
public abstract class BaseFilteredMonitoringDocTestCase<F extends FilteredMonitoringDoc> extends BaseMonitoringDocTestCase<F> {

    @Override
    protected final void assertMonitoringDoc(final F document) {
        assertFilteredMonitoringDoc(document);
        assertXContentFilters(document);
    }

    /**
     * Asserts that the specific fields of a {@link FilteredMonitoringDoc} have
     * the expected values.
     */
    protected abstract void assertFilteredMonitoringDoc(F document);

    /**
     * Returns the expected list of XContent filters for the {@link FilteredMonitoringDoc}
     */
    protected abstract Set<String> getExpectedXContentFilters();

    /**
     * Asserts that the XContent filters of a {@link FilteredMonitoringDoc} contains
     * the common filters and the expected custom ones.
     */
    private void assertXContentFilters(final F document) {
        final Set<String> expectedFilters = Sets.union(COMMON_XCONTENT_FILTERS, getExpectedXContentFilters());
        final Set<String> actualFilters = document.getFilters();

        expectedFilters.forEach(filter -> assertThat(actualFilters, hasItem(filter)));
        assertThat(actualFilters.size(), equalTo(expectedFilters.size()));
    }

    public void testConstructorFiltersMustNotBeNull() {
        expectThrows(
            NullPointerException.class,
            () -> new TestFilteredMonitoringDoc(cluster, timestamp, interval, node, system, type, id, null)
        );
    }

    public void testConstructorFiltersMustNotBeEmpty() {
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new TestFilteredMonitoringDoc(cluster, timestamp, interval, node, system, type, id, emptySet())
        );

        assertThat(e.getMessage(), equalTo("xContentFilters must not be empty"));
    }

    public void testFilteredMonitoringDocToXContent() throws IOException {
        final Set<String> filters = new HashSet<>(5);
        filters.add("_type.field_1");
        filters.add("_type.field_3");
        filters.add("_type.field_5.sub_*");

        final MonitoringDoc.Node node = new MonitoringDoc.Node("_uuid", "_host", "_addr", "_ip", "_name", 1504169190855L);
        final TestFilteredMonitoringDoc document = new TestFilteredMonitoringDoc(
            "_cluster",
            1502266739402L,
            1506593717631L,
            node,
            MonitoredSystem.ES,
            "_type",
            "_id",
            filters
        );

        final BytesReference xContent = XContentHelper.toXContent(document, XContentType.JSON, false);
        final String expected = """
            {
              "cluster_uuid": "_cluster",
              "timestamp": "2017-08-09T08:18:59.402Z",
              "interval_ms": 1506593717631,
              "type": "_type",
              "source_node": {
                "uuid": "_uuid",
                "host": "_host",
                "transport_address": "_addr",
                "ip": "_ip",
                "name": "_name",
                "timestamp": "2017-08-31T08:46:30.855Z"
              },
              "_type": {
                "field_1": 1,
                "field_3": {
                  "sub_field_3": 3
                },
                "field_5": [ { "sub_field_5": 5 } ]
              }
            }""";
        assertEquals(XContentHelper.stripWhitespace(expected), xContent.utf8ToString());
    }

    class TestFilteredMonitoringDoc extends FilteredMonitoringDoc {

        TestFilteredMonitoringDoc(
            final String cluster,
            final long timestamp,
            final long intervalMillis,
            final Node node,
            final MonitoredSystem system,
            final String type,
            final String id,
            final Set<String> xContentFilters
        ) {
            super(cluster, timestamp, intervalMillis, node, system, type, id, xContentFilters);
        }

        @Override
        protected void innerToXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(getType());
            {
                builder.field("field_1", 1);
                builder.field("field_2", 2);

                builder.startObject("field_3");
                builder.field("sub_field_3", 3);
                builder.endObject();

                builder.field("field_4", 4);

                builder.startArray("field_5");
                {
                    builder.startObject();
                    builder.field("sub_field_5", 5);
                    builder.endObject();

                    builder.startObject();
                    builder.field("other_field_5", 5);
                    builder.endObject();
                }
                builder.endArray();
            }
            builder.endObject();
        }
    }
}
