/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.datafeed;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * Utility methods used for datafeed configuration.
 */
public final class DatafeedConfigUtils {

    private DatafeedConfigUtils() {}

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
            || aggregationBuilder instanceof DateHistogramAggregationBuilder
            || isCompositeWithDateHistogramSource(aggregationBuilder);
    }

    public static boolean isCompositeWithDateHistogramSource(AggregationBuilder aggregationBuilder) {
        return aggregationBuilder instanceof CompositeAggregationBuilder
            && ((CompositeAggregationBuilder) aggregationBuilder).sources()
                .stream()
                .anyMatch(DateHistogramValuesSourceBuilder.class::isInstance);
    }

    public static DateHistogramValuesSourceBuilder getDateHistogramValuesSource(CompositeAggregationBuilder compositeAggregationBuilder) {
        for (CompositeValuesSourceBuilder<?> valuesSourceBuilder : compositeAggregationBuilder.sources()) {
            if (valuesSourceBuilder instanceof DateHistogramValuesSourceBuilder dateHistogramValuesSourceBuilder) {
                return dateHistogramValuesSourceBuilder;
            }
        }
        throw ExceptionsHelper.badRequestException("[composite] aggregations require exactly one [date_histogram] value source");
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
        if (histogramAggregation instanceof HistogramAggregationBuilder histo) {
            return (long) histo.interval();
        } else if (histogramAggregation instanceof DateHistogramAggregationBuilder dateHisto) {
            return validateAndGetDateHistogramInterval(DateHistogramAggOrValueSource.fromAgg(dateHisto));
        } else if (histogramAggregation instanceof CompositeAggregationBuilder composite) {
            return validateAndGetDateHistogramInterval(DateHistogramAggOrValueSource.fromCompositeAgg(composite));
        } else {
            throw new IllegalStateException("Invalid histogram aggregation [" + histogramAggregation.getName() + "]");
        }
    }

    /**
     * Returns the date histogram interval as epoch millis if valid, or throws
     * an {@link ElasticsearchException} with the validation error
     */
    private static long validateAndGetDateHistogramInterval(DateHistogramAggOrValueSource dateHistogram) {
        if (dateHistogram.timeZone() != null && dateHistogram.timeZone().normalized().equals(ZoneOffset.UTC) == false) {
            throw ExceptionsHelper.badRequestException("ML requires date_histogram.time_zone to be UTC");
        }

        // TODO retains `dateHistogramInterval()`/`interval()` access for bwc logic, needs updating
        if (dateHistogram.getCalendarInterval() != null) {
            return validateAndGetCalendarInterval(dateHistogram.getCalendarInterval().toString());
        } else if (dateHistogram.getFixedInterval() != null) {
            return dateHistogram.getFixedInterval().estimateMillis();
        } else {
            throw new IllegalArgumentException("Must specify an interval for date_histogram");
        }
    }

    public static long validateAndGetCalendarInterval(String calendarInterval) {
        TimeValue interval;
        Rounding.DateTimeUnit dateTimeUnit = DateHistogramAggregationBuilder.DATE_FIELD_UNITS.get(calendarInterval);
        if (dateTimeUnit != null) {
            interval = switch (dateTimeUnit) {
                case WEEK_OF_WEEKYEAR -> new TimeValue(7, TimeUnit.DAYS);
                case DAY_OF_MONTH -> new TimeValue(1, TimeUnit.DAYS);
                case HOUR_OF_DAY -> new TimeValue(1, TimeUnit.HOURS);
                case MINUTE_OF_HOUR -> new TimeValue(1, TimeUnit.MINUTES);
                case SECOND_OF_MINUTE -> new TimeValue(1, TimeUnit.SECONDS);
                case MONTH_OF_YEAR, YEAR_OF_CENTURY, QUARTER_OF_YEAR, YEARS_OF_CENTURY, MONTHS_OF_YEAR -> throw ExceptionsHelper
                    .badRequestException(invalidDateHistogramCalendarIntervalMessage(calendarInterval));
            };
        } else {
            interval = TimeValue.parseTimeValue(calendarInterval, "date_histogram.calendar_interval");
        }
        if (interval.days() > 7) {
            throw ExceptionsHelper.badRequestException(invalidDateHistogramCalendarIntervalMessage(calendarInterval));
        }
        return interval.millis();
    }

    private static String invalidDateHistogramCalendarIntervalMessage(String interval) {
        throw ExceptionsHelper.badRequestException(
            "When specifying a date_histogram calendar interval ["
                + interval
                + "], ML does not accept intervals longer than a week because of "
                + "variable lengths of periods greater than a week"
        );
    }

    private static class DateHistogramAggOrValueSource {

        static DateHistogramAggOrValueSource fromAgg(DateHistogramAggregationBuilder agg) {
            return new DateHistogramAggOrValueSource(agg, null);
        }

        static DateHistogramAggOrValueSource fromCompositeAgg(CompositeAggregationBuilder compositeAggregationBuilder) {
            return new DateHistogramAggOrValueSource(null, getDateHistogramValuesSource(compositeAggregationBuilder));
        }

        private final DateHistogramAggregationBuilder agg;
        private final DateHistogramValuesSourceBuilder sourceBuilder;

        private DateHistogramAggOrValueSource(DateHistogramAggregationBuilder agg, DateHistogramValuesSourceBuilder sourceBuilder) {
            assert agg != null || sourceBuilder != null;
            this.agg = agg;
            this.sourceBuilder = sourceBuilder;
        }

        private ZoneId timeZone() {
            return agg != null ? agg.timeZone() : sourceBuilder.timeZone();
        }

        private DateHistogramInterval getFixedInterval() {
            return agg != null ? agg.getFixedInterval() : sourceBuilder.getIntervalAsFixed();
        }

        private DateHistogramInterval getCalendarInterval() {
            return agg != null ? agg.getCalendarInterval() : sourceBuilder.getIntervalAsCalendar();
        }
    }
}
