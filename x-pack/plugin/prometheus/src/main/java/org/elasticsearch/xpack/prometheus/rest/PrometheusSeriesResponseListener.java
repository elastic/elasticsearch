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
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts an {@link EsqlQueryResponse} from a {@link org.elasticsearch.xpack.esql.plan.logical.TsInfo} plan into the
 * Prometheus {@code /api/v1/series} JSON response format.
 */
public class PrometheusSeriesResponseListener implements ActionListener<EsqlQueryResponse> {

    private static final Logger logger = LogManager.getLogger(PrometheusSeriesResponseListener.class);

    static final String COL_METRIC_NAME = "metric_name";
    static final String COL_DIMENSIONS = "dimensions";
    private static final String LABELS_PREFIX = "labels.";
    private static final String CONTENT_TYPE = "application/json";

    private final RestChannel channel;

    public PrometheusSeriesResponseListener(RestChannel channel) {
        this.channel = channel;
    }

    @Override
    public void onResponse(EsqlQueryResponse response) {
        // Do NOT close/decRef the response here: the framework (via respondAndRelease) calls
        // decRef() after this method returns, which is the correct single release.
        try {
            List<Map<String, String>> seriesList = extractSeries(response);
            sendSuccess(seriesList);
        } catch (Exception e) {
            logger.debug("Failed to build series response", e);
            PrometheusErrorResponse.send(channel, e, logger);
        }
    }

    @Override
    public void onFailure(Exception e) {
        logger.debug("Series query failed", e);
        PrometheusErrorResponse.send(channel, e, logger);
    }

    private static List<Map<String, String>> extractSeries(EsqlQueryResponse response) {
        var columns = response.columns();
        int metricNameCol = -1;
        int dimensionsCol = -1;
        for (int i = 0; i < columns.size(); i++) {
            String name = columns.get(i).name();
            if (COL_METRIC_NAME.equals(name)) {
                metricNameCol = i;
            } else if (COL_DIMENSIONS.equals(name)) {
                dimensionsCol = i;
            }
        }
        if (metricNameCol == -1 || dimensionsCol == -1) {
            throw new IllegalArgumentException(
                "TsInfo response is missing required columns [" + COL_METRIC_NAME + ", " + COL_DIMENSIONS + "]"
            );
        }
        final int metricNameIdx = metricNameCol;
        final int dimensionsIdx = dimensionsCol;
        List<Map<String, String>> result = new ArrayList<>();
        for (Iterable<Object> row : response.rows()) {
            String metricName = null;
            String dimensionsJson = null;
            int col = 0;
            for (Object value : row) {
                if (col == metricNameIdx) {
                    metricName = value != null ? value.toString() : null;
                } else if (col == dimensionsIdx) {
                    dimensionsJson = value != null ? value.toString() : null;
                }
                col++;
            }
            Map<String, String> labels = buildLabelMap(metricName, dimensionsJson);
            if (labels.isEmpty() == false) {
                result.add(labels);
            }
        }
        return result;
    }

    /**
     * Builds the label map for one TsInfo row. Parses {@code dimensionsJson} and, when
     * {@code dimensions} carries no {@code __name__} entry (OTel metrics), synthesises it
     * from the {@code metric_name} column.
     */
    static Map<String, String> buildLabelMap(String metricName, String dimensionsJson) {
        Map<String, String> labels = parseDimensions(dimensionsJson);
        // OTel metrics have no labels.__name__ in dimensions — synthesise it from metric_name
        if (labels.containsKey("__name__") == false && metricName != null) {
            labels.put("__name__", metricName);
        }
        assert labels.isEmpty() == false
            : "label map must not be empty for metric_name=[" + metricName + "] dimensions=[" + dimensionsJson + "]";
        return labels;
    }

    /**
     * Parses the {@code dimensions} JSON object and strips the {@code labels.} prefix from keys.
     * Example: {@code {"labels.__name__":"up","labels.job":"prometheus"}}
     * → {@code {"__name__":"up","job":"prometheus"}}
     */
    static Map<String, String> parseDimensions(String json) {
        Map<String, String> labels = new LinkedHashMap<>();
        if (json == null || json.isBlank()) {
            return labels;
        }
        // Simple JSON object parser for {"key":"value",...} – all string-typed values
        // Use xcontent for robust parsing
        try (var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json)) {
            parser.nextToken(); // START_OBJECT
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                String rawKey = parser.currentName();
                parser.nextToken();
                String value = parser.text();
                String labelName = rawKey.startsWith(LABELS_PREFIX) ? rawKey.substring(LABELS_PREFIX.length()) : rawKey;
                labels.put(labelName, value);
            }
        } catch (IOException e) {
            logger.debug("Failed to parse dimensions JSON [{}]", json, e);
        }
        return labels;
    }

    private void sendSuccess(List<Map<String, String>> seriesList) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        builder.field("status", "success");
        builder.startArray("data");
        for (Map<String, String> labels : seriesList) {
            builder.startObject();
            for (Map.Entry<String, String> entry : labels.entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        channel.sendResponse(new RestResponse(RestStatus.OK, CONTENT_TYPE, Strings.toString(builder)));
    }

}
