/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.rollup.job;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.xpack.core.rollup.RollupField;
import org.elasticsearch.xpack.core.rollup.job.DateHistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.GroupConfig;
import org.elasticsearch.xpack.core.rollup.job.RollupIndexerJobStats;
import org.elasticsearch.xpack.rollup.Rollup;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

/**
 * These utilities are used to convert agg responses into a set of rollup documents.
 * They are extracted out as static classes mainly to make testing easier.
 */
class IndexerUtils {
    private static final Logger logger = LogManager.getLogger(IndexerUtils.class);

    /**
     * The only entry point in this class.  You hand this method an aggregation and an index
     * pattern, and it returns a list of rolled documents that you can index
     *
     * @param agg              The aggregation response that you want to rollup
     * @param rollupIndex      The index that holds rollups for this job
     * @param stats            The stats accumulator for this job's task
     * @param groupConfig      The grouping configuration for the job
     * @param jobId            The ID for the job
     * @param isUpgradedDocID  `true` if this job is using the new ID scheme
     * @return                 A stream of rolled documents derived from the response
     */
    static Stream<IndexRequest> processBuckets(CompositeAggregation agg, String rollupIndex, RollupIndexerJobStats stats,
                                               GroupConfig groupConfig, String jobId, boolean isUpgradedDocID) {

        logger.debug("Buckets: [" + agg.getBuckets().size() + "][" + jobId + "]");
        return agg.getBuckets().stream().map(b ->{
            stats.incrementNumDocuments(b.getDocCount());

            // Put the composite keys into a treemap so that the key iteration order is consistent
            // TODO would be nice to avoid allocating this treemap in the future
            TreeMap<String, Object> keys = new TreeMap<>(b.getKey());
            List<Aggregation> metrics = b.getAggregations().asList();

            RollupIDGenerator idGenerator;
            if (isUpgradedDocID) {
                idGenerator = new RollupIDGenerator.Murmur3(jobId);
            } else  {
                idGenerator = new RollupIDGenerator.CRC();
            }
            Map<String, Object> doc = new HashMap<>(keys.size() + metrics.size());

            processKeys(keys, doc, b.getDocCount(), groupConfig, idGenerator);
            idGenerator.add(jobId);
            processMetrics(metrics, doc);

            doc.put(RollupField.ROLLUP_META + "." + RollupField.VERSION_FIELD,
                isUpgradedDocID ? Rollup.CURRENT_ROLLUP_VERSION : Rollup.ROLLUP_VERSION_V1);
            doc.put(RollupField.ROLLUP_META + "." + RollupField.ID.getPreferredName(), jobId);

            IndexRequest request = new IndexRequest(rollupIndex, RollupField.TYPE_NAME, idGenerator.getID());
            request.source(doc);
            return request;
        });
    }

    private static void processKeys(Map<String, Object> keys, Map<String, Object> doc,
                                     long count, GroupConfig groupConfig, RollupIDGenerator idGenerator) {
        keys.forEach((k, v) -> {
            // Also add a doc count for each key.  This will duplicate data, but makes search easier later
            doc.put(k + "." + RollupField.COUNT_FIELD, count);

            if (k.endsWith("." + DateHistogramAggregationBuilder.NAME)) {
                assert v != null;
                doc.put(k + "." + RollupField.TIMESTAMP, v);
                doc.put(k  + "." + RollupField.INTERVAL, groupConfig.getDateHistogram().getInterval());
                doc.put(k  + "." + DateHistogramGroupConfig.TIME_ZONE, groupConfig.getDateHistogram().getTimeZone());
                idGenerator.add((Long)v);
            } else if (k.endsWith("." + HistogramAggregationBuilder.NAME)) {
                doc.put(k + "." + RollupField.VALUE, v);
                doc.put(k + "." + RollupField.INTERVAL, groupConfig.getHistogram().getInterval());
                if (v == null) {
                    idGenerator.addNull();
                } else {
                    idGenerator.add((Double) v);
                }
            } else if (k.endsWith("." + TermsAggregationBuilder.NAME)) {
                doc.put(k + "." + RollupField.VALUE, v);
                if (v == null) {
                    idGenerator.addNull();
                } else if (v instanceof String) {
                    idGenerator.add((String)v);
                } else if (v instanceof Long) {
                    idGenerator.add((Long)v);
                } else if (v instanceof Double) {
                    idGenerator.add((Double)v);
                } else {
                    throw new RuntimeException("Encountered value of type ["
                        + v.getClass() + "], which was unable to be processed.");
                }
            } else {
                throw new ElasticsearchException("Could not identify key in agg [" + k + "]");
            }
        });
    }

    private static void processMetrics(List<Aggregation> metrics, Map<String, Object> doc) {
        List<String> emptyCounts = new ArrayList<>();
        metrics.forEach(m -> {
            if (m instanceof InternalNumericMetricsAggregation.SingleValue) {
                Double value = ((InternalNumericMetricsAggregation.SingleValue) m).value();
                if (value.isInfinite() == false) {
                    if (m.getName().endsWith(RollupField.COUNT_FIELD) && value == 0) {
                        emptyCounts.add(m.getName());
                    } else {
                        doc.put(m.getName(), value);
                    }
                }
            } else {
                throw new ElasticsearchException("Aggregation [" + m.getName() + "] is of non-supported type [" + m.getType() + "]");
            }
        });

        // Go back through and remove all empty counts
        emptyCounts.forEach(m -> doc.remove(m.replace(RollupField.COUNT_FIELD, RollupField.VALUE)));
    }
}
