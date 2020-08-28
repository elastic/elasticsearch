/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.datafeed.extractor;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.time.ZoneOffset;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * Collects common utility methods needed by various {@link DataExtractor} implementations
 */
public final class ExtractorUtils {

    private static final String EPOCH_MILLIS = "epoch_millis";

    private ExtractorUtils() {}

    /**
     * Combines a user query with a time range query.
     */
    public static QueryBuilder wrapInTimeRangeQuery(QueryBuilder userQuery, String timeField, long start, long end) {
        QueryBuilder timeQuery = new RangeQueryBuilder(timeField).gte(start).lt(end).format(EPOCH_MILLIS);
        return new BoolQueryBuilder().filter(userQuery).filter(timeQuery);
    }

    /**
     * Find the (date) histogram in {@code aggFactory} and extract its interval.
     * Throws if there is no (date) histogram or if the histogram has sibling
     * aggregations.
     * @param aggFactory Aggregations factory
     * @return The histogram interval
     */
    public static long getHistogramIntervalMillis(AggregatorFactories.Builder aggFactory) {
        AggregationBuilder histogram = getHistogramAggregation(aggFactory.getAggregatorFactories());
        return getHistogramIntervalMillis(histogram);
    }

    /**
     * Find and return (date) histogram in {@code aggregations}
     * @param aggregations List of aggregations
     * @return A {@link HistogramAggregationBuilder} or a {@link DateHistogramAggregationBuilder}
     */
    public static AggregationBuilder getHistogramAggregation(Collection<AggregationBuilder> aggregations) {
        if (aggregations.isEmpty()) {
            throw ExceptionsHelper.badRequestException(Messages.getMessage(Messages.DATAFEED_AGGREGATIONS_REQUIRES_DATE_HISTOGRAM));
        }
        if (aggregations.size() != 1) {
            throw ExceptionsHelper.badRequestException(Messages.DATAFEED_AGGREGATIONS_REQUIRES_DATE_HISTOGRAM_NO_SIBLINGS);
        }

        AggregationBuilder agg = aggregations.iterator().next();
        if (isHistogram(agg)) {
            return agg;
        } else {
            return getHistogramAggregation(agg.getSubAggregations());
        }
    }

    public static boolean isHistogram(AggregationBuilder aggregationBuilder) {
        return aggregationBuilder instanceof HistogramAggregationBuilder
                || aggregationBuilder instanceof DateHistogramAggregationBuilder;
    }

    /**
     * Get the interval from {@code histogramAggregation} or throw an {@code IllegalStateException}
     * if {@code histogramAggregation} is not a {@link HistogramAggregationBuilder} or a
     * {@link DateHistogramAggregationBuilder}
     *
     * @param histogramAggregation Must be a {@link HistogramAggregationBuilder} or a
     * {@link DateHistogramAggregationBuilder}
     * @return The histogram interval
     */
    public static long getHistogramIntervalMillis(AggregationBuilder histogramAggregation) {
        if (histogramAggregation instanceof HistogramAggregationBuilder) {
            return (long) ((HistogramAggregationBuilder) histogramAggregation).interval();
        } else if (histogramAggregation instanceof DateHistogramAggregationBuilder) {
            return validateAndGetDateHistogramInterval((DateHistogramAggregationBuilder) histogramAggregation);
        } else {
            throw new IllegalStateException("Invalid histogram aggregation [" + histogramAggregation.getName() + "]");
        }
    }

    /**
     * Returns the date histogram interval as epoch millis if valid, or throws
     * an {@link ElasticsearchException} with the validation error
     */
    private static long validateAndGetDateHistogramInterval(DateHistogramAggregationBuilder dateHistogram) {
        if (dateHistogram.timeZone() != null && dateHistogram.timeZone().normalized().equals(ZoneOffset.UTC) == false) {
            throw ExceptionsHelper.badRequestException("ML requires date_histogram.time_zone to be UTC");
        }

        // TODO retains `dateHistogramInterval()`/`interval()` access for bwc logic, needs updating
        if (dateHistogram.getCalendarInterval() != null) {
            return validateAndGetCalendarInterval(dateHistogram.getCalendarInterval().toString());
        } else if (dateHistogram.getFixedInterval() != null) {
            return dateHistogram.getFixedInterval().estimateMillis();
        } else if (dateHistogram.dateHistogramInterval() != null) {
            return validateAndGetCalendarInterval(dateHistogram.dateHistogramInterval().toString());
        } else if (dateHistogram.interval() != 0) {
            return dateHistogram.interval();
        } else {
            throw new IllegalArgumentException("Must specify an interval for DateHistogram");
        }
    }

    public static long validateAndGetCalendarInterval(String calendarInterval) {
        TimeValue interval;
        Rounding.DateTimeUnit dateTimeUnit = DateHistogramAggregationBuilder.DATE_FIELD_UNITS.get(calendarInterval);
        if (dateTimeUnit != null) {
            switch (dateTimeUnit) {
                case WEEK_OF_WEEKYEAR:
                    interval = new TimeValue(7, TimeUnit.DAYS);
                    break;
                case DAY_OF_MONTH:
                    interval = new TimeValue(1, TimeUnit.DAYS);
                    break;
                case HOUR_OF_DAY:
                    interval = new TimeValue(1, TimeUnit.HOURS);
                    break;
                case MINUTES_OF_HOUR:
                    interval = new TimeValue(1, TimeUnit.MINUTES);
                    break;
                case SECOND_OF_MINUTE:
                    interval = new TimeValue(1, TimeUnit.SECONDS);
                    break;
                case MONTH_OF_YEAR:
                case YEAR_OF_CENTURY:
                case QUARTER_OF_YEAR:
                    throw ExceptionsHelper.badRequestException(invalidDateHistogramCalendarIntervalMessage(calendarInterval));
                default:
                    throw ExceptionsHelper.badRequestException("Unexpected dateTimeUnit [" + dateTimeUnit + "]");
            }
        } else {
            interval = TimeValue.parseTimeValue(calendarInterval, "date_histogram.interval");
        }
        if (interval.days() > 7) {
            throw ExceptionsHelper.badRequestException(invalidDateHistogramCalendarIntervalMessage(calendarInterval));
        }
        return interval.millis();
    }

    private static String invalidDateHistogramCalendarIntervalMessage(String interval) {
        throw ExceptionsHelper.badRequestException("When specifying a date_histogram calendar interval ["
                + interval + "], ML does not accept intervals longer than a week because of " +
                "variable lengths of periods greater than a week");
    }

}
