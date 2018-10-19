/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.aggregation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
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

        // This should never happen as we are working with RollupJobCaps internally
        if (rollupJobConfigs.contains(null)) {
            List<RollupJobCaps> rollupJobCaps = rollupJobsWithCaps.values()
                .stream()
                .flatMap(rollableIndexCaps -> rollableIndexCaps.getJobCaps().stream())
                .collect(Collectors.toList());
            rollupJobCaps.forEach(rollupJobCap -> {
                if (rollupJobCap.getRollupJobConfig() == null) {
                    listener.onFailure(new IllegalArgumentException(
                        "missing rollup job config found for rollup index " +
                            "["+ rollupJobCap.getRollupIndex() +"] for rollup job ["+ rollupJobCap.getJobID() +"]"));
                }
            });
        }

        AggregationValidator aggregationValidator = new AggregationValidator(datafeed, rollupJobConfigs);
        List<String> validationErrors = aggregationValidator.validate();
        if (validationErrors.isEmpty()) {
            listener.onResponse(new RollupDataExtractorFactory(client, datafeed, job));
        } else {
            listener.onFailure(new IllegalArgumentException(Strings.collectionToDelimitedString(validationErrors, " ")));
        }
    }

    /**
     * Helper class for validating Aggregations between the {@link DatafeedConfig} and {@link RollupJobConfig} objects
     */
    private static class AggregationValidator {

        private final AggregationBuilder datafeedHistogramAggregation;
        private final Collection<AggregationBuilder> datafeedAggregations;
        private final Set<RollupJobConfig> rollupJobConfigs;
        private final Set<String> supportedTerms;
        private final Set<String> supportedMetrics;

        AggregationValidator(DatafeedConfig datafeed, Set<RollupJobConfig> rollupJobConfigs) {
            // Has to be a date_histogram for aggregation
            datafeedHistogramAggregation = getHistogramAggregation(datafeed.getAggregations().getAggregatorFactories());
            datafeedAggregations = datafeed.getAggregations().getAggregatorFactories();
            this.rollupJobConfigs = rollupJobConfigs;
            Set<String> metricSet = null;
            Set<String> termSets = null;
            for (RollupJobConfig rollupJobConfig : rollupJobConfigs) {
                Set<String> jobConfigsMetrics = new HashSet<>();
                Set<String> jobConfigTerms = new HashSet<>();
                rollupJobConfig.getMetricsConfig().forEach(metricConfig -> {
                    String field = metricConfig.getField();
                    metricConfig.getMetrics().forEach(metric -> jobConfigsMetrics.add(field + "_" + metric));
                });
                if (rollupJobConfig.getGroupConfig().getTerms() != null) {
                    jobConfigTerms.addAll(Arrays.asList(rollupJobConfig.getGroupConfig().getTerms().getFields()));
                }

                // Gets the intersection as the desired aggs need to be supported by all referenced rollup indexes
                if (metricSet == null) {
                    metricSet = jobConfigsMetrics;
                } else {
                    metricSet.retainAll(jobConfigsMetrics);
                }
                if (termSets == null) {
                    termSets = jobConfigTerms;
                } else {
                    termSets.retainAll(jobConfigTerms);
                }
            }
            supportedTerms = termSets;
            supportedMetrics = metricSet;
        }

        List<String> validate() {
            List<String> validationErrors = new ArrayList<>();
            validateDatafeedHistogramAgg(validationErrors);
            validateIntervals(validationErrors);
            validateAggregations(validationErrors);
            return validationErrors;
        }

        private void validateDatafeedHistogramAgg(List<String> validationErrors) {
            if ((datafeedHistogramAggregation instanceof DateHistogramAggregationBuilder) == false) {
                validationErrors.add("Rollup requires that the datafeed configuration use a [date_histogram] aggregation," +
                    " not a [histogram] aggregation over the time field.");
            }
        }

        private void validateIntervals(List<String> validationErrors) {
            long datafeedInterval = getHistogramIntervalMillis(datafeedHistogramAggregation);
            for (RollupJobConfig rollupJobConfig : rollupJobConfigs) {
                long interval = validateAndGetCalendarInterval(rollupJobConfig.getGroupConfig()
                    .getDateHistogram()
                    .getInterval()
                    .toString());
                if (datafeedInterval % interval > 0) {
                    validationErrors.add(
                        "Rollup Job [" + rollupJobConfig.getId() + "] has an interval that is not a multiple of the dataframe interval.");
                }
            }
        }

        private void validateAggregations(List<String> validationErrors) {
            List<String> aggregationErrors = new ArrayList<>();
            verifyAggregationsHelper(datafeedAggregations, aggregationErrors);
            if (aggregationErrors.isEmpty() == false) {
                validationErrors.add("Rollup indexes do not support the following aggregations: " +
                    Strings.collectionToCommaDelimitedString(aggregationErrors) + ".");
            }
        }

        private void verifyAggregationsHelper(final Collection<AggregationBuilder> datafeedAggregations,
                                              final List<String> aggregationErrors) {
            for (AggregationBuilder aggregationBuilder : datafeedAggregations) {
                if (aggregationBuilder.equals(datafeedHistogramAggregation) == false) {
                    String type = aggregationBuilder.getType();
                    String field = ((ValuesSourceAggregationBuilder) aggregationBuilder).field();
                    if (aggregationBuilder instanceof TermsAggregationBuilder) {
                        if (supportedTerms.contains(field) == false) {
                            aggregationErrors.add("[terms] for field [" + field + "]");
                        }
                    } else {
                        if (supportedMetrics.contains(field + "_" + type) == false) {
                            aggregationErrors.add("[" + type + "] for field [" + field + "]");
                        }
                    }
                }
                verifyAggregationsHelper(aggregationBuilder.getSubAggregations(), aggregationErrors);
            }
        }
    }
}
