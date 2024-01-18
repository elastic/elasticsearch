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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.SearchInterval;
import org.elasticsearch.xpack.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.ql.util.DateUtils;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class EsqlDataExtractor implements DataExtractor {

    private static final Logger logger = LogManager.getLogger(EsqlDataExtractor.class);

    private final Client client;
    private final DatafeedConfig datafeed;
    private final String timeField;
    private final SearchInterval interval;
    private boolean isCancelled;

    EsqlDataExtractor(Client client, DatafeedConfig datafeed, String timeField, long start, long end) {
        this.client = Objects.requireNonNull(client);
        this.datafeed = Objects.requireNonNull(datafeed);
        this.timeField = timeField;
        this.interval = new SearchInterval(start, end);
        this.isCancelled = false;
    }

    // TODO: check whether these expressions facilitate injection attacks!
    private String esqlTimeFilter() {
        return Strings.format(
            " | WHERE %s >= \"%s\" AND %s < \"%s\"",
            timeField,
            DateUtils.UTC_DATE_TIME_FORMATTER.formatMillis(
                Math.min(interval.startMs(), org.elasticsearch.common.time.DateUtils.MAX_MILLIS_BEFORE_9999)
            ),
            timeField,
            DateUtils.UTC_DATE_TIME_FORMATTER.formatMillis(
                Math.min(interval.endMs(), org.elasticsearch.common.time.DateUtils.MAX_MILLIS_BEFORE_9999)
            )
        );
    }

    private String esqlSortByTime() {
        return Strings.format(" | SORT %s", timeField);
    }

    private String esqlSummaryStats() {
        return Strings.format(" | STATS earliest_time=MIN(%s), latest_time=MAX(%s), total_hits=COUNT(*)", timeField, timeField);
    }

    @Override
    public DataSummary getSummary() {
        EsqlQueryRequest request = new EsqlQueryRequest();
        request.query(datafeed.getEsqlQuery() + esqlTimeFilter() + esqlSummaryStats());

        try (EsqlQueryResponse response = execute(request)) {
            Iterator<Object> values = response.values().next();
            String earliestTime = (String) values.next();
            String latestTime = (String) values.next();
            long totalHits = (long) values.next();
            return new DataSummary(
                earliestTime == null ? null : DateUtils.UTC_DATE_TIME_FORMATTER.parseMillis(earliestTime),
                latestTime == null ? null : DateUtils.UTC_DATE_TIME_FORMATTER.parseMillis(latestTime),
                totalHits
            );
        }
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Result next() throws IOException {
        EsqlQueryRequest request = new EsqlQueryRequest();
        request.query(datafeed.getEsqlQuery() + esqlTimeFilter() + esqlSortByTime());

        EsqlQueryResponse response = execute(request);

        try (BytesStreamOutput outputStream = new BytesStreamOutput()) {
            XContentBuilder jsonBuilder = new XContentBuilder(JsonXContent.jsonXContent, outputStream);

            List<ColumnInfo> columns = response.columns();
            int valueCount = 0;
            for (Iterator<Iterator<Object>> itValues = response.values(); itValues.hasNext();) {
                jsonBuilder.startObject();
                int index = 0;
                for (Iterator<Object> itValue = itValues.next(); itValue.hasNext();) {
                    Object value = itValue.next();
                    if ("date".equals(columns.get(index).type())) {
                        if (value instanceof String && Strings.isNullOrEmpty((String) value) == false) {
                            value = DateUtils.UTC_DATE_TIME_FORMATTER.parseMillis((String) value);
                        }
                        // TODO: something with arrays of dates? (e.g. kibana_sample_data_ecommerce -> products.created_on)
                    }
                    jsonBuilder.field(columns.get(index).name(), value);
                    index++;
                }
                jsonBuilder.endObject();
                valueCount++;
            }
            jsonBuilder.close();

            logger.info(
                "query interval: {} - {}, valueCount: {}",
                DateUtils.UTC_DATE_TIME_FORMATTER.formatMillis(interval.startMs()),
                DateUtils.UTC_DATE_TIME_FORMATTER.formatMillis(interval.endMs()),
                valueCount
            );

            return new Result(interval, Optional.of(outputStream.bytes().streamInput()));
        }
    }

    @Override
    public boolean isCancelled() {
        return isCancelled;
    }

    @Override
    public void cancel() {
        logger.trace("Data extractor received cancel request");
        isCancelled = true;
    }

    @Override
    public void destroy() {}

    @Override
    public long getEndTime() {
        return interval.endMs();
    }

    private EsqlQueryResponse execute(EsqlQueryRequest request) {
        return ClientHelper.executeWithHeaders(
            datafeed.getHeaders(),
            ClientHelper.ML_ORIGIN,
            client,
            () -> client.execute(EsqlQueryAction.INSTANCE, request).actionGet()
        );
    }
}
