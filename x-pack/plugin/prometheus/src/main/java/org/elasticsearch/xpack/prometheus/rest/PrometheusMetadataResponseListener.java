/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Converts an {@link EsqlQueryResponse} into the Prometheus {@code /api/v1/metadata} JSON response format.
 *
 * <p>Each row from the Aggregate has 3 columns: {@code metric_name} (0), {@code metric_type} (1), {@code unit} (2).
 * The {@code metrics.} storage prefix is stripped from {@code metric_name}: metrics stored in the
 * {@code metrics.*} field namespace (the common case for both Prometheus and OTel data) carry the raw ES
 * field path (e.g. {@code metrics.up}). Metrics without a passthrough field do not carry this prefix
 * and are returned as-is. Help is always returned as an empty string.
 *
 * <p>Truncation uses the {@code limit + 1} sentinel on distinct metric names: if the response listener encounters
 * {@code limit + 1} distinct metric names, it truncates to {@code limit} and emits a warning. The
 * {@code limit_per_metric} parameter caps the number of entries per metric name.
 */
public class PrometheusMetadataResponseListener {

    private static final Logger logger = LogManager.getLogger(PrometheusMetadataResponseListener.class);

    private static final String METRICS_PREFIX = "metrics.";
    private static final String CONTENT_TYPE = "application/json";

    private PrometheusMetadataResponseListener() {}

    public static ActionListener<EsqlQueryResponse> create(RestChannel channel, int limit, int limitPerMetric) {
        return ActionListener.<Void>wrap(ignored -> {}, e -> {
            logger.debug("Metadata query failed", e);
            PrometheusErrorResponse.send(channel, e, logger);
        }).delegateFailureAndWrap((l, response) -> {
            LinkedHashMap<String, List<MetadataEntry>> entries = collectEntries(response.rows(), limit, limitPerMetric);
            channel.sendResponse(buildSuccessResponse(entries, limit));
        });
    }

    /**
     * Collects metadata entries from the 3-column ESQL result rows. Strips the {@code metrics.} storage prefix
     * from metric names: metrics stored in the {@code metrics.*} field namespace carry the raw ES field path;
     * metrics without a passthrough field do not carry this prefix and are returned as-is.
     * Applies {@code limit} and {@code limitPerMetric} caps. Package-private for testing.
     */
    static LinkedHashMap<String, List<MetadataEntry>> collectEntries(
        Iterable<? extends Iterable<Object>> rows,
        int limit,
        int limitPerMetric
    ) {
        LinkedHashMap<String, List<MetadataEntry>> result = new LinkedHashMap<>();
        for (Iterable<Object> row : rows) {
            Iterator<Object> cols = row.iterator();
            Object rawName = cols.next();
            Object rawType = cols.next();
            Object rawUnit = cols.next();

            if (rawName == null) {
                continue;
            }

            String metricName = rawName.toString();
            if (metricName.startsWith(METRICS_PREFIX)) {
                metricName = metricName.substring(METRICS_PREFIX.length());
            }
            if (metricName.isEmpty()) {
                continue;
            }

            if (result.containsKey(metricName) == false) {
                // Collect at most limit+1 distinct metric names as a sentinel for truncation detection
                if (limit > 0 && result.size() >= limit + 1) {
                    continue;
                }
                result.put(metricName, new ArrayList<>());
            }

            List<MetadataEntry> entries = result.get(metricName);
            if (limitPerMetric > 0 && entries.size() >= limitPerMetric) {
                continue;
            }

            entries.add(new MetadataEntry(rawType != null ? rawType.toString() : "", rawUnit != null ? rawUnit.toString() : ""));
        }
        return result;
    }

    /**
     * Builds the Prometheus metadata success response. Uses the {@code limit + 1} sentinel to detect truncation.
     * Package-private for testing.
     */
    static RestResponse buildSuccessResponse(LinkedHashMap<String, List<MetadataEntry>> entries, int limit) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        builder.field("status", "success");
        builder.startObject("data");

        var iterator = entries.entrySet().iterator();
        int total = limit > 0 ? Math.min(limit, entries.size()) : entries.size();
        for (int written = 0; written < total; written++) {
            var entry = iterator.next();
            builder.startArray(entry.getKey());
            for (MetadataEntry metadata : entry.getValue()) {
                builder.startObject();
                builder.field("type", metadata.type());
                builder.field("help", "");
                builder.field("unit", metadata.unit());
                builder.endObject();
            }
            builder.endArray();
        }

        builder.endObject();

        boolean truncated = limit > 0 && entries.size() > limit;
        if (truncated) {
            builder.startArray("warnings");
            builder.value("results truncated due to limit");
            builder.endArray();
        }

        builder.endObject();
        return new RestResponse(RestStatus.OK, CONTENT_TYPE, Strings.toString(builder));
    }

    record MetadataEntry(String type, String unit) {}
}
