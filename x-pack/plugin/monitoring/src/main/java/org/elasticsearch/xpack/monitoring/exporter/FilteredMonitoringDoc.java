/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.exporter;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;

import java.io.IOException;
import java.util.Set;

/**
 * {@link FilteredMonitoringDoc} are a kind of {@link MonitoringDoc} whose XContent
 * is filtered when the document is printed out.
 */
public abstract class FilteredMonitoringDoc extends MonitoringDoc {

    /**
     * List of common XContent fields that exist in all monitoring documents
     */
    static final Set<String> COMMON_XCONTENT_FILTERS = Set.of("cluster_uuid", "timestamp", "interval_ms", "type", "source_node");

    private final Set<String> filters;

    public FilteredMonitoringDoc(
        final String cluster,
        final long timestamp,
        final long intervalMillis,
        @Nullable final Node node,
        final MonitoredSystem system,
        final String type,
        @Nullable final String id,
        final Set<String> xContentFilters
    ) {
        super(cluster, timestamp, intervalMillis, node, system, type, id);
        if (xContentFilters.isEmpty()) {
            throw new IllegalArgumentException("xContentFilters must not be empty");
        }

        filters = Sets.union(COMMON_XCONTENT_FILTERS, xContentFilters);
    }

    Set<String> getFilters() {
        return filters;
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            try (XContentBuilder filteredBuilder = new XContentBuilder(builder.contentType(), out, filters)) {
                super.toXContent(filteredBuilder, params);
            }
            try (
                XContentParser parser = XContentHelper.createParserNotCompressed(
                    LoggingDeprecationHandler.XCONTENT_PARSER_CONFIG,
                    out.bytes(),
                    builder.contentType()
                )
            ) {
                return builder.copyCurrentStructure(parser);
            }
        }
    }
}
