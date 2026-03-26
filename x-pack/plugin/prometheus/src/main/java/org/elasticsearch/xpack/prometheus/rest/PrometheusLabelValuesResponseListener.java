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
import java.util.List;

/**
 * Converts an {@link EsqlQueryResponse} into the
 * Prometheus {@code /api/v1/label/{name}/values} JSON response format.
 *
 * <p>ESQL has already sorted and deduplicated the values via {@code STATS BY} and {@code ORDER BY}.
 * This listener iterates the rows and strips storage prefixes before returning values:
 * <ul>
 *   <li>{@code labels.} prefix — ESQL resolves the passthrough field to its stored path
 *       {@code labels.job} for Prometheus data; OTel stores without prefix so no stripping needed.</li>
 *   <li>{@code metrics.} prefix — the {@code metric_name} column returned by MetricsInfo for the
 *       {@code __name__} plan branch carries the raw ES field path, e.g. {@code metrics.up}.</li>
 * </ul>
 *
 * <p>Truncation detection uses a {@code limit + 1} sentinel: if the result contains exactly
 * {@code limit + 1} rows, the last row is dropped and a warning is emitted.
 *
 * <p>When ESQL returns a {@code "Unknown column"} BAD_REQUEST error (label name absent from all
 * index mappings), the listener converts it into an empty {@code data:[]} success response — the
 * correct Prometheus behaviour for a label with no values.
 */
public class PrometheusLabelValuesResponseListener {

    private static final Logger logger = LogManager.getLogger(PrometheusLabelValuesResponseListener.class);

    private static final String LABELS_PREFIX = "labels.";
    private static final String METRICS_PREFIX = "metrics.";
    private static final String CONTENT_TYPE = "application/json";

    private PrometheusLabelValuesResponseListener() {}

    public static ActionListener<EsqlQueryResponse> create(RestChannel channel, int limit) {
        // Do NOT close/decRef the response here: the framework (via respondAndRelease) calls
        // decRef() after this method returns, which is the correct single release.
        return ActionListener.<Void>wrap(ignored -> {}, e -> {
            logger.debug("Label values query failed", e);
            // When ESQL cannot find the label name in any index mapping it throws a BAD_REQUEST
            // "Unknown column [<name>]" error. The correct Prometheus response for a label that has
            // no values is an empty data array, not an error.
            if (isUnknownColumn(e)) {
                try {
                    channel.sendResponse(buildSuccessResponse(List.of(), 0));
                } catch (Exception sendEx) {
                    sendEx.addSuppressed(e);
                    logger.warn("Failed to send empty-data response for unknown column", sendEx);
                }
            } else {
                PrometheusErrorResponse.send(channel, e, logger);
            }
        }).delegateFailureAndWrap((l, response) -> {
            List<String> values = collectValues(response.rows());
            channel.sendResponse(buildSuccessResponse(values, limit));
        });
    }

    /**
     * Collects label values from the single-column ESQL result rows, stripping storage prefixes:
     * {@code "labels."} (Prometheus passthrough field) and {@code "metrics."} (raw ES field path
     * returned by MetricsInfo for the {@code __name__} plan branch). OTel values carry neither
     * prefix and are returned as-is. Package-private for testing.
     */
    static List<String> collectValues(Iterable<? extends Iterable<Object>> rows) {
        List<String> result = new ArrayList<>();
        for (Iterable<Object> row : rows) {
            Object value = row.iterator().next();
            if (value == null) {
                continue;
            }
            String raw = value.toString();
            String labelValue;
            if (raw.startsWith(LABELS_PREFIX)) {
                labelValue = raw.substring(LABELS_PREFIX.length());
            } else if (raw.startsWith(METRICS_PREFIX)) {
                labelValue = raw.substring(METRICS_PREFIX.length());
            } else {
                labelValue = raw;
            }
            if (labelValue.isEmpty() == false) {
                result.add(labelValue);
            }
        }
        return result;
    }

    /**
     * Builds the success response. Uses the {@code limit + 1} sentinel to detect truncation:
     * if collected size equals {@code limit + 1}, truncates to {@code limit} and adds a warning.
     * Package-private for testing.
     */
    static RestResponse buildSuccessResponse(List<String> values, int limit) throws IOException {
        boolean truncated = false;
        List<String> output = values;
        if (limit > 0 && values.size() == limit + 1) {
            output = values.subList(0, limit);
            truncated = true;
        }
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        builder.field("status", "success");
        builder.startArray("data");
        for (String v : output) {
            builder.value(v);
        }
        builder.endArray();
        if (truncated) {
            builder.startArray("warnings");
            builder.value("results truncated due to limit");
            builder.endArray();
        }
        builder.endObject();
        return new RestResponse(RestStatus.OK, CONTENT_TYPE, Strings.toString(builder));
    }

    /**
     * Returns {@code true} if {@code e} is an ESQL analysis error caused by a label name that is
     * absent from all index mappings. ESQL produces a {@code "Unknown column [name]"} message in
     * its {@code VerificationException}. When no TS indices exist at all, the error may be
     * combined with an {@code "@timestamp"} unresolved message and arrive as a 500; both cases
     * mean no data exists for the requested label, so an empty {@code data:[]} response is correct.
     */
    static boolean isUnknownColumn(Exception e) {
        return e != null && e.getMessage() != null && e.getMessage().contains("Unknown column [");
    }
}
