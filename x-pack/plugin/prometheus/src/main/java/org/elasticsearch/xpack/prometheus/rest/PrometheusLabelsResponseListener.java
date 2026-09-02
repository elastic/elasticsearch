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
 * Converts an {@link EsqlQueryResponse} from a Prometheus labels ESQL plan into the
 * Prometheus {@code /api/v1/labels} JSON response format.
 *
 * <p>After the plan executes, the response has a single {@code dimension_fields} column with one
 * label name per row (already deduplicated and sorted by the ESQL plan). Values have the
 * {@code "labels."} prefix stripped before being returned. OTel dimension fields carry no such
 * prefix (e.g. {@code "attributes.service.name"}) and are returned as-is.
 *
 * <p>{@code __name__} is always emitted as the first label in the response. For Prometheus metrics
 * it is present in {@code dimension_fields} as {@code "labels.__name__"} and is deduplicated by
 * the serialisation. For OTel metrics it is absent from {@code dimension_fields} but is injected
 * here to signal to clients (e.g. Grafana) that the metric-name label is always available.
 */
public class PrometheusLabelsResponseListener {

    private static final Logger logger = LogManager.getLogger(PrometheusLabelsResponseListener.class);

    private static final String LABELS_PREFIX = "labels.";
    private static final String NAME_LABEL = "__name__";
    private static final String CONTENT_TYPE = "application/json";

    private PrometheusLabelsResponseListener() {}

    static ActionListener<EsqlQueryResponse> create(RestChannel channel, int limit) {
        return ActionListener.wrap(response -> {
            List<String> labelNames = collectLabelNames(response.rows());
            channel.sendResponse(buildSuccessResponse(labelNames, limit));
        }, e -> {
            logger.debug("Labels request failed", e);
            PrometheusErrorResponse.send(channel, e, logger);
        });
    }

    /**
     * Collects label names from the single-column ESQL result rows, stripping the
     * {@code "labels."} prefix. OTel dimension fields (e.g. {@code "attributes.service.name"})
     * carry no such prefix and are returned as-is. Package-private for testing.
     */
    static List<String> collectLabelNames(Iterable<? extends Iterable<Object>> rows) {
        List<String> result = new ArrayList<>();
        for (Iterable<Object> row : rows) {
            Object value = row.iterator().next();
            if (value == null) {
                continue;
            }
            String raw = value.toString();
            String labelName = raw.startsWith(LABELS_PREFIX) ? raw.substring(LABELS_PREFIX.length()) : raw;
            if (labelName.isEmpty() == false) {
                result.add(labelName);
            }
        }
        return result;
    }

    /**
     * Builds the success response. {@code __name__} is always written first (synthetic for OTel,
     * deduped for Prometheus). Truncation detection uses the sentinel approach: the plan builder
     * requests {@code limit+1} rows from ESQL; if exactly {@code limit+1} rows are returned the
     * result was truncated and a {@code warnings} entry is added. The extra sentinel row is never
     * included in the output. When {@code limit == 0} (unlimited) no truncation check is performed.
     * Package-private for testing.
     */
    static RestResponse buildSuccessResponse(List<String> labelNames, int limit) throws IOException {
        boolean truncated = limit > 0 && labelNames.size() == limit + 1;
        // When truncated the last entry is the sentinel row, exclude it from the output
        List<String> names = truncated ? labelNames.subList(0, limit) : labelNames;
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        builder.field("status", "success");
        builder.startArray("data");
        // __name__ is always first — it sorts before 'a' in ASCII so this is also correct order
        builder.value(NAME_LABEL);
        for (String name : names) {
            if (NAME_LABEL.equals(name) == false) {
                builder.value(name);
            }
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

}
