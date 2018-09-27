/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.rollup;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
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
import org.elasticsearch.xpack.core.rollup.action.RollupJobCaps;
import org.elasticsearch.xpack.core.rollup.job.RollupJobConfig;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;
import org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.AggregationDataExtractorContext;

import java.util.ArrayList;
import java.util.Arrays;
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
            datafeedConfig.getQuery(),
            datafeedConfig.getAggregations(),
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


        Set<RollupJobConfig> rollupJobConfigs = rollupJobsWithCaps.values()
            .stream()
            .flatMap(rollableIndexCaps -> rollableIndexCaps.getJobCaps().stream())
            .map(RollupJobCaps::getRollupJobConfig)
            .collect(Collectors.toSet());

        AggregationValidator aggregationValidator = new AggregationValidator(datafeed, rollupJobConfigs);

        String histogramError = aggregationValidator.validateDatafeedHistogramAgg();
        if (histogramError != null) {
            listener.onFailure(new IllegalArgumentException(histogramError));
            return;
        }

        String intervalError = aggregationValidator.validateIntervals();
        if (intervalError != null) {
            listener.onFailure(new IllegalArgumentException(intervalError));
            return;
        }
        String aggregationError = aggregationValidator.validateAggregations();
        if (aggregationError != null) {
            listener.onFailure(new IllegalArgumentException(aggregationError));
            return;
        }

        listener.onResponse(new RollupDataExtractorFactory(client, datafeed, job));
    }

    /**
     * Helper class for validating Aggregations between the {@link DatafeedConfig} and {@link RollupJobConfig} objects
     */
    private static class AggregationValidator {

        final private AggregationBuilder datafeedHistogramAggregation;
        final long datafeedInterval;
        final String timeField;
        List<AggregationBuilder> datafeedAggregations;
        final Set<RollupJobConfig> rollupJobConfigs;
        final Set<String> supportedTerms;
        final Set<String> supportedMetrics;
        final List<String> aggregationErrors = new ArrayList<>();

        private AggregationValidator(DatafeedConfig datafeed, Set<RollupJobConfig> rollupJobConfigs) {
            // Has to be a date_histogram for aggregation
            datafeedHistogramAggregation = getHistogramAggregation(datafeed.getAggregations().getAggregatorFactories());
            datafeedInterval = getHistogramIntervalMillis(datafeedHistogramAggregation);
            timeField = ((ValuesSourceAggregationBuilder) datafeedHistogramAggregation).field();
            datafeedAggregations = datafeed.getAggregations().getAggregatorFactories();
            this.rollupJobConfigs = rollupJobConfigs;
            List<Set<String>> metricSets = new ArrayList<>();
            List<Set<String>> termSets = new ArrayList<>();
            rollupJobConfigs.forEach(rollupJobConfig -> {
                Set<String> jobConfigsMetrics = new HashSet<>();
                Set<String> jobConfigTerms = new HashSet<>();
                rollupJobConfig.getMetricsConfig().forEach(metricConfig -> {
                    String field = metricConfig.getField();
                    metricConfig.getMetrics().forEach(metric -> jobConfigsMetrics.add(field + "_" + metric));
                });
                if (rollupJobConfig.getGroupConfig().getTerms() != null) {
                    jobConfigTerms.addAll(Arrays.asList(rollupJobConfig.getGroupConfig().getTerms().getFields()));
                }
                metricSets.add(jobConfigsMetrics);
                termSets.add(jobConfigTerms);
            });
            supportedTerms = termSets.stream().reduce(new HashSet<>(termSets.get(0)), (lft, rgt) -> {
                lft.retainAll(rgt);
                return lft;
            });
            supportedMetrics = metricSets.stream().reduce(new HashSet<>(metricSets.get(0)), (lft, rgt) -> {
                lft.retainAll(rgt);
                return lft;
            });
        }

        private String validateDatafeedHistogramAgg() {
            if (datafeedHistogramAggregation instanceof DateHistogramAggregationBuilder) {
                return null;
            }
            return "Rollup requires that the datafeed configuration use a [date_histogram] aggregation," +
                " not a [histogram] aggregation over the time field.";
        }

        private String validateIntervals() {
            for (RollupJobConfig rollupJobConfig : rollupJobConfigs) {
                long interval = validateAndGetCalendarInterval(rollupJobConfig.getGroupConfig()
                    .getDateHistogram()
                    .getInterval()
                    .toString());
                if (datafeedInterval % interval > 0) {
                    return "Rollup Job [" + rollupJobConfig.getId() + "] has an interval that is not a multiple of the dataframe interval";
                }
            }
            return null;
        }

        private String validateAggregations() {
            verifyAggregationsHelper(datafeedAggregations, false);
            if (aggregationErrors.isEmpty()) {
                return null;
            }
            return "Rollup indexes do not support: " + Strings.collectionToCommaDelimitedString(aggregationErrors);
        }

        private void verifyAggregationsHelper(final List<AggregationBuilder> datafeedAggregations, final boolean parentIsHistogram) {
            for (AggregationBuilder aggregationBuilder : datafeedAggregations) {
                String type = aggregationBuilder.getType();
                String field = ((ValuesSourceAggregationBuilder) aggregationBuilder).field();
                // The required max Time field for the base histogram
                if (parentIsHistogram && field.equals(timeField) && type.equals("max")) {
                    continue;
                }
                // what about buckets?
                if ((aggregationBuilder instanceof HistogramAggregationBuilder ||
                    aggregationBuilder instanceof DateHistogramAggregationBuilder) == false) {
                    if (aggregationBuilder instanceof TermsAggregationBuilder) {
                        if (supportedTerms.contains(field) == false) {
                            aggregationErrors.add("terms aggregation for term [" + field + "]");
                        }
                    } else {
                        if (supportedMetrics.contains(field + "_" + type) == false) {
                            aggregationErrors.add("metric [" + type + "] for field [" + field + "]");
                        }
                    }
                }
                verifyAggregationsHelper(aggregationBuilder.getSubAggregations(), aggregationBuilder.equals(datafeedHistogramAggregation));
            }
        }
    }

}
