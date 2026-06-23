/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.core.esql.action.EsqlResponse;
import org.elasticsearch.xpack.core.ml.datafeed.SearchInterval;
import org.elasticsearch.xpack.ml.datafeed.DatafeedTimingStatsReporter;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractor;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

public class EsqlDataExtractor implements DataExtractor {

    private static final Logger LOGGER = LogManager.getLogger(EsqlDataExtractor.class);

    private static final String EPOCH_MILLIS = "epoch_millis";

    private final Client client;
    private final EsqlDataExtractorContext context;
    private final DatafeedTimingStatsReporter timingStatsReporter;
    private boolean hasNext = true;
    private boolean isCancelled = false;

    public EsqlDataExtractor(Client client, EsqlDataExtractorContext context, DatafeedTimingStatsReporter timingStatsReporter) {
        this.client = Objects.requireNonNull(client);
        this.context = Objects.requireNonNull(context);
        this.timingStatsReporter = Objects.requireNonNull(timingStatsReporter);
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public boolean isCancelled() {
        return isCancelled;
    }

    @Override
    public void cancel() {
        LOGGER.trace("[{}] Data extractor received cancel request", context.jobId());
        isCancelled = true;
    }

    @Override
    public void destroy() {
        cancel();
    }

    @Override
    public long getEndTime() {
        return context.end();
    }

    @Override
    public DataSummary getSummary() {
        String summaryQuery = context.esqlQuery()
            + " | STATS earliest_time = MIN(`"
            + context.timeField()
            + "`), latest_time = MAX(`"
            + context.timeField()
            + "`), total_hits = COUNT(*)";
        QueryBuilder timeFilter = buildTimeFilter();
        long startMs = client.threadPool().relativeTimeInMillis();
        try (EsqlQueryResponse response = runEsqlQuery(summaryQuery, timeFilter)) {
            long durationMs = client.threadPool().relativeTimeInMillis() - startMs;
            timingStatsReporter.reportSearchDuration(TimeValue.timeValueMillis(durationMs));
            return parseSummaryResponse(response.response());
        }
    }

    private QueryBuilder buildTimeFilter() {
        return new RangeQueryBuilder(context.timeField()).gte(context.start()).lt(context.end()).format(EPOCH_MILLIS);
    }

    /**
     * Reads the single row produced by the summary {@code STATS} query and converts it into a
     * {@link DataSummary}. The query projection is fixed: {@code earliest_time} (column 0),
     * {@code latest_time} (column 1), {@code total_hits} (column 2).
     * <p>
     * {@code STATS} without {@code BY} always emits exactly one row, even when no documents match
     * (in which case min/max are {@code null} and count is {@code 0}). The {@code null} min maps
     * to a {@code null} {@code earliestTime}, so {@link DataSummary#hasData()} returns {@code false}
     * and the chunker treats the range as empty — identical to the previous DSL path's behaviour.
     */
    private static DataSummary parseSummaryResponse(EsqlResponse response) {
        List<? extends ColumnInfo> columns = response.columns();
        // MIN/MAX inherit the time field's output type; detect it once from the first column.
        boolean timeIsDate = columns.isEmpty() == false
            && ("date".equals(columns.get(0).outputType()) || "date_nanos".equals(columns.get(0).outputType()));
        for (Iterable<Object> row : response.rows()) {
            List<Object> values = new ArrayList<>();
            for (Object v : row) {
                values.add(v);
            }
            Long earliestTime = toEpochMillisOrNull(values.get(0), timeIsDate);
            Long latestTime = toEpochMillisOrNull(values.get(1), timeIsDate);
            long totalHits = values.get(2) instanceof Number n ? n.longValue() : 0L;
            return new DataSummary(earliestTime, latestTime, totalHits);
        }
        // Defensive fallback — STATS without BY should never produce zero rows.
        return new DataSummary(null, null, 0L);
    }

    private static Long toEpochMillisOrNull(Object value, boolean isDate) {
        if (value == null) {
            return null;
        }
        return isDate ? toEpochMillis((String) value) : ((Number) value).longValue();
    }

    @Override
    public Result next() throws IOException {
        if (hasNext() == false) {
            throw new NoSuchElementException();
        }
        hasNext = false;
        SearchInterval searchInterval = new SearchInterval(context.start(), context.end());
        if (isCancelled) {
            return new Result(searchInterval, Optional.empty(), List.of());
        }

        QueryBuilder timeFilter = buildTimeFilter();

        // Anomaly detection drops records that arrive out of time order
        // We add a SORT on the time field to ensure that the data is returned in time order
        String orderedQuery = context.esqlQuery() + " | SORT `" + context.timeField() + "` ASC";

        try (EsqlQueryResponse response = runEsqlQuery(orderedQuery, timeFilter)) {
            Optional<InputStream> data = toNdjson(response.response(), context.timeField(), context.requiredSummaryCountField());
            return new Result(searchInterval, data, List.of());
        }
    }

    protected EsqlQueryResponse runEsqlQuery(String orderedQuery, QueryBuilder timeFilter) {
        EsqlQueryRequestBuilder<? extends EsqlQueryRequest, ? extends EsqlQueryResponse> request = EsqlQueryRequestBuilder
            .newRequestBuilder(client)
            .query(orderedQuery)
            .filter(timeFilter);
        return execute(request);
    }

    private EsqlQueryResponse execute(EsqlQueryRequestBuilder<? extends EsqlQueryRequest, ? extends EsqlQueryResponse> request) {
        return ClientHelper.executeWithHeaders(context.headers(), ClientHelper.ML_ORIGIN, client, () -> request.execute().actionGet());
    }

    private static Optional<InputStream> toNdjson(EsqlResponse response, String timeField, String requiredSummaryCountField)
        throws IOException {
        List<? extends ColumnInfo> columns = response.columns();
        boolean[] isDateColumn = new boolean[columns.size()];
        boolean foundTimeField = false;
        boolean foundSummaryCountField = requiredSummaryCountField == null;
        for (int i = 0; i < columns.size(); i++) {
            String name = columns.get(i).name();
            String type = columns.get(i).outputType();
            isDateColumn[i] = "date".equals(type) || "date_nanos".equals(type);
            if (timeField.equals(name)) {
                foundTimeField = true;
            }
            if (requiredSummaryCountField != null && requiredSummaryCountField.equals(name)) {
                foundSummaryCountField = true;
            }
        }

        List<String> missingColumns = new ArrayList<>();
        if (!foundTimeField) {
            missingColumns.add(timeField);
        }
        if (!foundSummaryCountField) {
            missingColumns.add(requiredSummaryCountField);
        }
        if (!missingColumns.isEmpty()) {
            throw new IllegalArgumentException(
                "ESQL query response is missing the required columns: "
                    + String.join(", ", missingColumns)
                    + ". Ensure the query's final projection includes these columns."
            );
        }

        BytesStreamOutput out = new BytesStreamOutput();
        boolean hasRows = false;
        for (Iterable<Object> row : response.rows()) {
            hasRows = true;
            try (XContentBuilder b = XContentFactory.jsonBuilder(out)) {
                writeRow(b, row, columns, isDateColumn);
            }
            out.write('\n');
        }
        return hasRows ? Optional.of(out.bytes().streamInput()) : Optional.empty();
    }

    private static void writeRow(XContentBuilder b, Iterable<Object> row, List<? extends ColumnInfo> columns, boolean[] isDateColumn)
        throws IOException {
        b.startObject();
        int columnIndex = 0;
        for (Object value : row) {
            if (value != null) {
                writeField(b, columns.get(columnIndex).name(), value, isDateColumn[columnIndex]);
            }
            columnIndex++;
        }
        b.endObject();
    }

    private static void writeField(XContentBuilder b, String name, Object value, boolean isDate) throws IOException {
        if (isDate) {
            if (value instanceof List<?> list) {
                List<Long> epochMillis = new ArrayList<>(list.size());
                for (Object element : list) {
                    epochMillis.add(toEpochMillis((String) element));
                }
                b.field(name, epochMillis);
            } else {
                b.field(name, toEpochMillis((String) value));
            }
        } else if (value instanceof List<?> list) {
            b.field(name, list);
        } else {
            b.field(name, value);
        }
    }

    private static long toEpochMillis(String isoDate) {
        return Instant.parse(isoDate).toEpochMilli();
    }
}
