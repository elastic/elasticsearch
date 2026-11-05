/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.action.ResponseValueUtils;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Converts the result of an exemplars query (see {@link PrometheusQueryExemplarsRestAction}) into the Prometheus
 * {@code query_exemplars} response: the exemplars grouped by the series they belong to.
 * <pre>{@code
 * {"status":"success","data":[{"seriesLabels":{"__name__":"...",...},"exemplars":[{"labels":{"trace_id":"..."},"value":"6",
 *   "timestamp":1600096945.479}]}]}
 * }</pre>
 * The labels of a series are those of the {@code _timeseries} column, named like the query endpoints name them, see
 * {@link PrometheusQueryResponseListener#writeMetricFromSeriesJson}. The labels of an exemplar are its {@code trace_id} and
 * {@code span_id} plus the ones recorded with it: the {@code filtered_attributes} of OTLP exemplars and the {@code exemplar_labels} of
 * Prometheus remote write ones.
 *
 * @see <a href="https://prometheus.io/docs/prometheus/latest/querying/api/#querying-exemplars">Prometheus Querying Exemplars API</a>
 */
class PrometheusQueryExemplarsResponseListener implements ActionListener<EsqlQueryResponse> {

    private static final Logger logger = LogManager.getLogger(PrometheusQueryExemplarsResponseListener.class);

    static final String METRIC_NAME_COLUMN = "metric_name";
    static final String VALUE_COLUMN = "value";
    static final String TRACE_ID_COLUMN = "trace_id";
    static final String SPAN_ID_COLUMN = "span_id";

    /**
     * The objects of an exemplar's source holding the exemplar labels, currently hardcoded for our OTLP and Prom remote write schemas.
     * If required, we could make this a setting instead.
     * */
    private static final List<String> EXEMPLAR_LABEL_OBJECTS = List.of("filtered_attributes", "exemplar_labels");

    private final RestChannel channel;
    private final Instant start;
    private final Instant end;

    /**
     * @param start the start of the requested time range, inclusive
     * @param end the end of the requested time range, inclusive
     */
    PrometheusQueryExemplarsResponseListener(RestChannel channel, Instant start, Instant end) {
        this.channel = channel;
        this.start = start;
        this.end = end;
    }

    @Override
    public void onResponse(EsqlQueryResponse queryResponse) {
        // The response is released by the caller once this listener returns, see PrometheusQueryResponseListener#onResponse.
        try {
            XContentBuilder builder = convertToPrometheusJson(
                queryResponse.pages(),
                queryResponse.columns(),
                queryResponse.zoneId(),
                start,
                end
            );
            channel.sendResponse(new RestResponse(RestStatus.OK, builder));
        } catch (Exception e) {
            onFailure(e);
        }
    }

    @Override
    public void onFailure(Exception e) {
        PrometheusErrorResponse.send(channel, e, logger);
    }

    private record Exemplar(Map<String, String> labels, double value, long timestampMillis) {}

    /**
     * Converts the exemplars, which the exemplars query returns most recent first, into the Prometheus response. The evaluated time
     * range of the query extends before {@code start} by the lookback of its selectors, so the exemplars outside of
     * {@code [start, end]} are dropped here.
     */
    static XContentBuilder convertToPrometheusJson(
        List<Page> pages,
        List<ColumnInfoImpl> columns,
        ZoneId zoneId,
        Instant start,
        Instant end
    ) throws IOException {
        Columns cols = Columns.of(columns, zoneId);
        if (cols == null) {
            // no exemplars found
            return emptyResponse();
        }
        long startMillis = start.toEpochMilli();
        long endMillis = end.toEpochMilli();
        Map<Map<String, String>, List<Exemplar>> exemplarsBySeries = new HashMap<>();
        for (Page page : pages) {
            for (int position = 0; position < page.getPositionCount(); position++) {
                Long timestamp = cols.timestamp.readTimestampMillis(page, position);
                if (timestamp == null || timestamp < startMillis || timestamp > endMillis) {
                    // due to lookback built-in into to our PROMQL implementation we actually query longer time ranges
                    // so we make sure to manually filter out the exemplars outside of the desired range
                    continue;
                }
                Number value = cols.value.readNumber(page, position);
                if (value == null) {
                    continue;
                }
                Map<String, String> seriesLabels = seriesLabels(
                    cols.timeseries.readString(page, position),
                    cols.metricName.readString(page, position)
                );
                Map<String, String> exemplarLabels = exemplarLabels(
                    cols.source.readMap(page, position),
                    cols.traceId.readString(page, position),
                    cols.spanId.readString(page, position)
                );
                exemplarsBySeries.computeIfAbsent(seriesLabels, k -> new ArrayList<>())
                    .add(new Exemplar(exemplarLabels, value.doubleValue(), timestamp));
            }
        }
        return buildResponse(exemplarsBySeries);
    }

    /** The response of a query that selects no series. */
    static XContentBuilder emptyResponse() throws IOException {
        return buildResponse(Map.of());
    }

    /**
     * Writes the series and their exemplars, oldest first like Prometheus does. Like the query endpoints, neither the series nor the
     * labels are sorted.
     */
    private static XContentBuilder buildResponse(Map<Map<String, String>, List<Exemplar>> exemplarsBySeries) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.field("status", "success");
        builder.startArray("data");
        for (Map.Entry<Map<String, String>, List<Exemplar>> series : exemplarsBySeries.entrySet()) {
            List<Exemplar> exemplars = series.getValue();
            exemplars.sort(Comparator.comparingLong(Exemplar::timestampMillis));
            builder.startObject();
            builder.field("seriesLabels", series.getKey());
            builder.startArray("exemplars");
            for (Exemplar exemplar : exemplars) {
                builder.startObject();
                builder.field("labels", exemplar.labels());
                builder.field("value", PrometheusQueryResponseListener.formatSampleValue(exemplar.value()));
                builder.field("timestamp", PrometheusQueryResponseListener.parseTimestamp(exemplar.timestampMillis()));
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    /**
     * The labels of the series an exemplar belongs to, from its {@code _timeseries} dimensions. These are named like the query endpoints
     * name them (see {@link PrometheusQueryResponseListener#writeMetricFromSeriesJson}): the {@code labels} of Prometheus series without
     * their prefix, all other dimensions by their dot-separated path. {@code __name__} is always {@code metricName}, the metric of the
     * exemplar, so a {@code metric_name} or {@code __name__} dimension is dropped.
     */
    static Map<String, String> seriesLabels(@Nullable String timeseriesJson, String metricName) throws IOException {
        Map<String, String> labels = new HashMap<>();
        if (timeseriesJson != null) {
            Map<String, Object> dimensions = PrometheusQueryResponseListener.parseLabelsJson(timeseriesJson, true);
            // Remove a potential, synthetic metric_name dimension. The metric name will be added down below
            dimensions.remove(METRIC_NAME_COLUMN);
            dimensions.remove(LabelMatcher.NAME);
            PrometheusQueryResponseListener.forEachEntryFlattened(dimensions, (name, value) -> putLabel(labels, name, value));
        }
        putLabel(labels, LabelMatcher.NAME, metricName);
        return labels;
    }

    /**
     * The labels of an exemplar: the ones recorded with it in its source, plus its {@code trace_id} and {@code span_id}. OTLP exemplars
     * store these as fields of their own, Prometheus remote write ones as exemplar labels, which the columns resolve to as well.
     */
    static Map<String, String> exemplarLabels(@Nullable Map<?, ?> source, @Nullable String traceId, @Nullable String spanId)
        throws IOException {
        Map<String, String> labels = new HashMap<>();
        if (source != null) {
            for (String object : EXEMPLAR_LABEL_OBJECTS) {
                if (source.get(object) instanceof Map<?, ?> recordedLabels) {
                    PrometheusQueryResponseListener.forEachEntryFlattened(recordedLabels, (name, value) -> putLabel(labels, name, value));
                }
            }
        }
        if (traceId != null) {
            putLabel(labels, TRACE_ID_COLUMN, traceId);
        }
        if (spanId != null) {
            putLabel(labels, SPAN_ID_COLUMN, spanId);
        }
        return labels;
    }

    /** Adds a label with the value the query endpoints give it, see {@link PrometheusQueryResponseListener#labelValue}. */
    private static void putLabel(Map<String, String> labels, String name, Object value) {
        String labelValue = PrometheusQueryResponseListener.labelValue(value);
        if (labelValue != null) {
            labels.put(name, labelValue);
        }
    }

    /**
     * A column of an exemplars query. Its accessors read the first value at a position with the extractor of its type, return
     * {@code null} if there is none or the column is absent, and fail if the column has a type they don't expect.
     */
    private static final class Column {
        private static final Column ABSENT = new Column(-1, null, DataType.NULL, null);

        private final int index;
        private final String name;
        private final DataType type;
        private final ResponseValueUtils.BlockValueExtractor extractor;
        private final BytesRef scratch = new BytesRef();

        private Column(int index, String name, DataType type, ResponseValueUtils.BlockValueExtractor extractor) {
            this.index = index;
            this.name = name;
            this.type = type;
            this.extractor = extractor;
        }

        static Column of(@Nullable Integer index, List<ColumnInfoImpl> columns, ZoneId zoneId) {
            if (index == null) {
                return ABSENT;
            }
            ColumnInfoImpl column = columns.get(index);
            return new Column(index, column.name(), column.type(), ResponseValueUtils.valueExtractorFor(column.type(), zoneId));
        }

        private IllegalStateException unexpectedType() {
            return new IllegalStateException("exemplars column [" + name + "] has unexpected type [" + type + "]");
        }

        @Nullable
        private Block block(Page page, int position) {
            if (this == ABSENT) {
                return null;
            }
            Block block = page.getBlock(index);
            return block.isNull(position) ? null : block;
        }

        @Nullable
        private Object read(Page page, int position) {
            Block block = block(page, position);
            return block == null ? null : extractor.extract(block, block.getFirstValueIndex(position), scratch);
        }

        /** The value as a string, which is absent if empty. */
        @Nullable
        String readString(Page page, int position) {
            return switch (read(page, position)) {
                case null -> null;
                case String string -> string.isEmpty() ? null : string;
                default -> throw unexpectedType();
            };
        }

        /** The value of a column holding an object, such as {@code _source}. */
        @Nullable
        Map<?, ?> readMap(Page page, int position) {
            return switch (read(page, position)) {
                case null -> null;
                case Map<?, ?> map -> map;
                default -> throw unexpectedType();
            };
        }

        @Nullable
        Number readNumber(Page page, int position) {
            return switch (read(page, position)) {
                case null -> null;
                case Number number -> number;
                default -> throw unexpectedType();
            };
        }

        /** The value of a date column as epoch millis, which the extractor would format instead. */
        @Nullable
        Long readTimestampMillis(Page page, int position) {
            Block block = block(page, position);
            if (block == null) {
                return null;
            }
            TimeUnit unit = switch (type) {
                case DATETIME -> TimeUnit.MILLISECONDS;
                case DATE_NANOS -> TimeUnit.NANOSECONDS;
                default -> throw unexpectedType();
            };
            return unit.toMillis(((LongBlock) block).getLong(block.getFirstValueIndex(position)));
        }
    }

    /**
     * The columns of an exemplars query, see {@code ExemplarsRewriter#exemplarProjections}. Optional columns are
     * {@link Column#ABSENT} when absent.
     */
    private record Columns(
        Column timestamp,
        Column value,
        Column metricName,
        Column traceId,
        Column spanId,
        Column timeseries,
        Column source
    ) {
        @Nullable
        static Columns of(List<ColumnInfoImpl> columns, ZoneId zoneId) {
            Map<String, Integer> indices = new HashMap<>();
            for (int i = 0; i < columns.size(); i++) {
                indices.put(columns.get(i).name(), i);
            }
            Integer timestamp = indices.get(MetadataAttribute.TIMESTAMP_FIELD);
            Integer value = indices.get(VALUE_COLUMN);
            if (timestamp == null || value == null) {
                return null;
            }
            return new Columns(
                Column.of(timestamp, columns, zoneId),
                Column.of(value, columns, zoneId),
                Column.of(indices.get(METRIC_NAME_COLUMN), columns, zoneId),
                Column.of(indices.get(TRACE_ID_COLUMN), columns, zoneId),
                Column.of(indices.get(SPAN_ID_COLUMN), columns, zoneId),
                Column.of(indices.get(MetadataAttribute.TIMESERIES), columns, zoneId),
                Column.of(indices.get(SourceFieldMapper.NAME), columns, zoneId)
            );
        }
    }
}
