/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.scheduler.http;

import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Objects;

public class ElasticsearchQueryBuilder {

    /**
     * The search body for Elasticsearch version 2.x contains sorting
     * based on the time field and a query. The query is composed by
     * a bool query with two must clauses, the recommended way to perform an AND query.
     * There are 6 placeholders:
     * <ol>
     *   <li> sort field
     *   <li> user defined query
     *   <li> time field
     *   <li> start time (String in date_time format)
     *   <li> end time (String in date_time format)
     *   <li> extra (may be empty or contain aggregations, fields, etc.)
     * </ol>
     */
    private static final String SEARCH_BODY_TEMPLATE_2_X = "{"
            + "\"sort\": ["
            +   "{\"%s\": {\"order\": \"asc\"}}"
            + "],"
            + "\"query\": {"
            +   "\"bool\": {"
            +     "\"filter\": ["
            +       "{%s},"
            +       "{"
            +         "\"range\": {"
            +           "\"%s\": {"
            +             "\"gte\": \"%s\","
            +             "\"lt\": \"%s\","
            +             "\"format\": \"date_time\""
            +           "}"
            +         "}"
            +       "}"
            +     "]"
            +   "}"
            + "}%s"
            + "}";

    private static final String DATA_SUMMARY_SORT_FIELD = "_doc";

    /**
     * Aggregations in order to retrieve the earliest and latest record times.
     * The single placeholder expects the time field.
     */
    private static final String DATA_SUMMARY_AGGS_TEMPLATE = ""
            + "{"
            +   "\"earliestTime\":{"
            +     "\"min\":{\"field\":\"%1$s\"}"
            +   "},"
            +   "\"latestTime\":{"
            +     "\"max\":{\"field\":\"%1$s\"}"
            +   "}"
            + "}";

    private static final String AGGREGATION_TEMPLATE = ", \"aggs\": %s";
    private static final String SCRIPT_FIELDS_TEMPLATE = ", \"script_fields\": %s";
    private static final String FIELDS_TEMPLATE = "%s,  \"_source\": %s";

    private final String search;
    private final String aggregations;
    private final String scriptFields;
    private final String fields;
    private final String timeField;

    public ElasticsearchQueryBuilder(String search, String aggs, String scriptFields, String fields, String timeField) {
        this.search = Objects.requireNonNull(search);
        aggregations = aggs;
        this.scriptFields = scriptFields;
        this.fields = fields;
        this.timeField = Objects.requireNonNull(timeField);
    }

    public String createSearchBody(long start, long end) {
        return createSearchBody(start, end, timeField, aggregations);
    }

    private String createSearchBody(long start, long end, String sortField, String aggs) {
        return String.format(Locale.ROOT, SEARCH_BODY_TEMPLATE_2_X, sortField, search, timeField, formatAsDateTime(start),
                formatAsDateTime(end), createResultsFormatSpec(aggs));
    }

    private static String formatAsDateTime(long epochMs) {
        Instant instant = Instant.ofEpochMilli(epochMs);
        ZonedDateTime dateTime = ZonedDateTime.ofInstant(instant, ZoneOffset.UTC);
        return dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX", Locale.ROOT));
    }

    private String createResultsFormatSpec(String aggs) {
        return (aggs != null) ? createAggregations(aggs) : ((fields != null) ? createFieldDataFields() : "");
    }

    private String createAggregations(String aggs) {
        return String.format(Locale.ROOT, AGGREGATION_TEMPLATE, aggs);
    }

    private String createFieldDataFields() {
        return String.format(Locale.ROOT, FIELDS_TEMPLATE, createScriptFields(), fields);
    }

    private String createScriptFields() {
        return (scriptFields != null) ? String.format(Locale.ROOT, SCRIPT_FIELDS_TEMPLATE, scriptFields) : "";
    }

    public String createDataSummaryQuery(long start, long end) {
        String aggs = String.format(Locale.ROOT, DATA_SUMMARY_AGGS_TEMPLATE, timeField);
        return createSearchBody(start, end, DATA_SUMMARY_SORT_FIELD, aggs);
    }

    public void logQueryInfo(Logger logger) {
        if (aggregations != null) {
            logger.debug("Will use the following Elasticsearch aggregations: " + aggregations);
        } else {
            if (fields != null) {
                logger.debug("Will request only the following field(s) from Elasticsearch: " + String.join(" ", fields));
            } else {
                logger.debug("Will retrieve whole _source document from Elasticsearch");
            }
        }
    }

    public boolean isAggregated() {
        return aggregations != null;
    }
}
