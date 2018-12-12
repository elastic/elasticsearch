/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.aggregation;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.utils.Intervals;
import org.elasticsearch.xpack.core.rollup.action.RollableIndexCaps;
import org.elasticsearch.xpack.core.rollup.action.RollupJobCaps.RollupFieldCaps;
import org.elasticsearch.xpack.core.rollup.job.DateHistogramGroupConfig;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ml.datafeed.extractor.ExtractorUtils.getHistogramAggregation;
import static org.elasticsearch.xpack.core.ml.datafeed.extractor.ExtractorUtils.getHistogramIntervalMillis;
import static org.elasticsearch.xpack.core.ml.datafeed.extractor.ExtractorUtils.validateAndGetCalendarInterval;

public class RollupDataExtractorFactory implements DataExtractorFactory {

    private final Client client;
    private final DatafeedConfig datafeedConfig;
    private final Job job;

    private RollupDataExtractorFactory(Client client, DatafeedConfig datafeedConfig, Job job) {
        this.client = Objects.requireNonNull(client);
        this.datafeedConfig = Objects.requireNonNull(datafeedConfig);
        this.job = Objects.requireNonNull(job);
    }

    @Override
    public DataExtractor newExtractor(long start, long end) {
        long histogramInterval = datafeedConfig.getHistogramIntervalMillis();
        AggregationDataExtractorContext dataExtractorContext = new AggregationDataExtractorContext(
            job.getId(),
            job.getDataDescription().getTimeField(),
            job.getAnalysisConfig().analysisFields(),
            datafeedConfig.getIndices(),
            datafeedConfig.getTypes(),
            datafeedConfig.getParsedQuery(),
            datafeedConfig.getParsedAggregations(),
            Intervals.alignToCeil(start, histogramInterval),
            Intervals.alignToFloor(end, histogramInterval),
            job.getAnalysisConfig().getSummaryCountFieldName().equals(DatafeedConfig.DOC_COUNT),
            datafeedConfig.getHeaders());
        return new RollupDataExtractor(client, dataExtractorContext);
    }

    public static void create(Client client,
                              DatafeedConfig datafeed,
                              Job job,
                              Map<String, RollableIndexCaps> rollupJobsWithCaps,
                              ActionListener<DataExtractorFactory> listener) {

        final AggregationBuilder datafeedHistogramAggregation = getHistogramAggregation(
            datafeed.getParsedAggregations().getAggregatorFactories());
        if ((datafeedHistogramAggregation instanceof DateHistogramAggregationBuilder) == false) {
            listener.onFailure(
                new IllegalArgumentException("Rollup requires that the datafeed configuration use a [date_histogram] aggregation," +
                    " not a [histogram] aggregation over the time field."));
            return;
        }

        final String timeField = ((ValuesSourceAggregationBuilder) datafeedHistogramAggregation).field();

        Set<ParsedRollupCaps> rollupCapsSet = rollupJobsWithCaps.values()
            .stream()
            .flatMap(rollableIndexCaps -> rollableIndexCaps.getJobCaps().stream())
            .map(rollupJobCaps -> ParsedRollupCaps.fromJobFieldCaps(rollupJobCaps.getFieldCaps(), timeField))
            .collect(Collectors.toSet());

        final long datafeedInterval = getHistogramIntervalMillis(datafeedHistogramAggregation);

        List<ParsedRollupCaps> validIntervalCaps = rollupCapsSet.stream()
            .filter(rollupCaps -> validInterval(datafeedInterval, rollupCaps))
            .collect(Collectors.toList());

        if (validIntervalCaps.isEmpty()) {
            listener.onFailure(
                new IllegalArgumentException(
                    "Rollup capabilities do not have a [date_histogram] aggregation with an interval " +
                        "that is a multiple of the datafeed's interval.")
            );
            return;
        }
        final List<ValuesSourceAggregationBuilder> flattenedAggs = new ArrayList<>();
        flattenAggregations(datafeed.getParsedAggregations().getAggregatorFactories(), datafeedHistogramAggregation, flattenedAggs);

        if (validIntervalCaps.stream().noneMatch(rollupJobConfig -> hasAggregations(rollupJobConfig, flattenedAggs))) {
            listener.onFailure(
                new IllegalArgumentException("Rollup capabilities do not support all the datafeed aggregations at the desired interval.")
            );
            return;
        }

        listener.onResponse(new RollupDataExtractorFactory(client, datafeed, job));
    }

    private static boolean validInterval(long datafeedInterval, ParsedRollupCaps rollupJobGroupConfig) {
        if (rollupJobGroupConfig.hasDatehistogram() == false) {
            return false;
        }
        if ("UTC".equalsIgnoreCase(rollupJobGroupConfig.getTimezone()) == false) {
            return false;
        }
        try {
            long jobInterval = validateAndGetCalendarInterval(rollupJobGroupConfig.getInterval());
            return datafeedInterval % jobInterval == 0;
        } catch (ElasticsearchStatusException exception) {
            return false;
        }
    }

    private static void flattenAggregations(final Collection<AggregationBuilder> datafeedAggregations,
                                            final AggregationBuilder datafeedHistogramAggregation,
                                            final List<ValuesSourceAggregationBuilder> flattenedAggregations) {
        for (AggregationBuilder aggregationBuilder : datafeedAggregations) {
            if (aggregationBuilder.equals(datafeedHistogramAggregation) == false) {
                flattenedAggregations.add((ValuesSourceAggregationBuilder)aggregationBuilder);
            }
            flattenAggregations(aggregationBuilder.getSubAggregations(), datafeedHistogramAggregation, flattenedAggregations);
        }
    }

    private static boolean hasAggregations(ParsedRollupCaps rollupCaps, List<ValuesSourceAggregationBuilder> datafeedAggregations) {
        for (ValuesSourceAggregationBuilder aggregationBuilder : datafeedAggregations) {
            String type = aggregationBuilder.getType();
            String field = aggregationBuilder.field();
            if (aggregationBuilder instanceof TermsAggregationBuilder) {
                if (rollupCaps.supportedTerms.contains(field) == false) {
                    return false;
                }
            } else {
                if (rollupCaps.supportedMetrics.contains(field + "_" + type) == false) {
                    return false;
                }
            }
        }
        return true;
    }

    private static class ParsedRollupCaps {
        private final Set<String> supportedMetrics;
        private final Set<String> supportedTerms;
        private final Map<String, Object> datehistogramAgg;
        private static final List<String> aggsToIgnore =
            Arrays.asList(HistogramAggregationBuilder.NAME, DateHistogramAggregationBuilder.NAME);

        private static ParsedRollupCaps fromJobFieldCaps(Map<String, RollupFieldCaps> rollupFieldCaps, String timeField) {
            Map<String, Object> datehistogram = null;
            RollupFieldCaps timeFieldCaps = rollupFieldCaps.get(timeField);
            if (timeFieldCaps != null) {
                for(Map<String, Object> agg : timeFieldCaps.getAggs()) {
                    if (agg.get("agg").equals(DateHistogramAggregationBuilder.NAME)) {
                        datehistogram = agg;
                    }
                }
            }
            Set<String> supportedMetrics = new HashSet<>();
            Set<String> supportedTerms = new HashSet<>();
            rollupFieldCaps.forEach((field, fieldCaps) -> {
                fieldCaps.getAggs().forEach(agg -> {
                    String type = (String)agg.get("agg");
                    if (type.equals(TermsAggregationBuilder.NAME)) {
                        supportedTerms.add(field);
                    } else if (aggsToIgnore.contains(type) == false) {
                        supportedMetrics.add(field + "_" + type);
                    }
                });
            });
            return new ParsedRollupCaps(supportedMetrics, supportedTerms, datehistogram);
        }

        private ParsedRollupCaps(Set<String> supportedMetrics, Set<String> supportedTerms, Map<String, Object> datehistogramAgg) {
            this.supportedMetrics = supportedMetrics;
            this.supportedTerms = supportedTerms;
            this.datehistogramAgg = datehistogramAgg;
        }

        private String getInterval() {
            if (datehistogramAgg == null) {
                return null;
            }
            return (String)datehistogramAgg.get(DateHistogramGroupConfig.INTERVAL);
        }

        private String getTimezone() {
            if (datehistogramAgg == null) {
                return null;
            }
            return (String)datehistogramAgg.get(DateHistogramGroupConfig.TIME_ZONE);
        }

        private boolean hasDatehistogram() {
            return datehistogramAgg != null;
        }
    }
}
