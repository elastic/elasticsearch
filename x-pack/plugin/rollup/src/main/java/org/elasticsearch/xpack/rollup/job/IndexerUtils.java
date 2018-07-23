/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup.job;

import org.apache.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.xpack.core.rollup.RollupField;
import org.elasticsearch.xpack.core.rollup.job.DateHistoGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.GroupConfig;
import org.elasticsearch.xpack.core.rollup.job.RollupJobStats;
import org.elasticsearch.xpack.rollup.Rollup;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

/**
 * These utilities are used to convert agg responses into a set of rollup documents.
 * They are extracted out as static classes mainly to make testing easier.
 */
class IndexerUtils {
    private static final Logger logger = Logger.getLogger(IndexerUtils.class.getName());

    /**
     * The only entry point in this class.  You hand this method an aggregation and an index
     * pattern, and it returns a list of rolled documents that you can index
     *
     * @param agg          The aggregation response that you want to rollup
     * @param rollupIndex  The index that holds rollups for this job
     * @return             A list of rolled documents derived from the response
     */
    static List<IndexRequest> processBuckets(CompositeAggregation agg, String rollupIndex, RollupJobStats stats,
                                             GroupConfig groupConfig, String jobId) {

        logger.debug("Buckets: [" + agg.getBuckets().size() + "][" + jobId + "]");
        return agg.getBuckets().stream().map(b ->{
            stats.incrementNumDocuments(b.getDocCount());

            // Put the composite keys into a treemap so that the key iteration order is consistent
            // TODO would be nice to avoid allocating this treemap in the future
            TreeMap<String, Object> keys = new TreeMap<>(b.getKey());
            List<Aggregation> metrics = b.getAggregations().asList();

            Map<String, Object> doc = new HashMap<>(keys.size() + metrics.size());
            CRC32 docId = processKeys(keys, doc, b.getDocCount(), groupConfig);
            byte[] vs = jobId.getBytes(StandardCharsets.UTF_8);
            docId.update(vs, 0, vs.length);
            processMetrics(metrics, doc);

            doc.put(RollupField.ROLLUP_META + "." + RollupField.VERSION_FIELD, Rollup.ROLLUP_VERSION);
            doc.put(RollupField.ROLLUP_META + "." + RollupField.ID.getPreferredName(), jobId);

            IndexRequest request = new IndexRequest(rollupIndex, RollupField.TYPE_NAME, String.valueOf(docId.getValue()));
            request.source(doc);
            return request;
        }).collect(Collectors.toList());
    }

    private static CRC32 processKeys(Map<String, Object> keys, Map<String, Object> doc, long count, GroupConfig groupConfig) {
        CRC32 docID = new CRC32();

        keys.forEach((k, v) -> {
            // Also add a doc count for each key.  This will duplicate data, but makes search easier later
            doc.put(k + "." + RollupField.COUNT_FIELD, count);

            if (k.endsWith("." + DateHistogramAggregationBuilder.NAME)) {
                assert v != null;
                doc.put(k + "." + RollupField.TIMESTAMP, v);
                doc.put(k  + "." + RollupField.INTERVAL, groupConfig.getDateHisto().getInterval());
                doc.put(k  + "." + DateHistoGroupConfig.TIME_ZONE, groupConfig.getDateHisto().getTimeZone().toString());
                docID.update(Numbers.longToBytes((Long)v), 0, 8);
            } else if (k.endsWith("." + HistogramAggregationBuilder.NAME)) {
                doc.put(k + "." + RollupField.VALUE, v);
                doc.put(k + "." + RollupField.INTERVAL, groupConfig.getHisto().getInterval());
                if (v == null) {
                    // Arbitrary value to update the doc ID with for nulls
                    docID.update(19);
                } else {
                    docID.update(Numbers.doubleToBytes((Double) v), 0, 8);
                }
            } else if (k.endsWith("." + TermsAggregationBuilder.NAME)) {
                doc.put(k + "." + RollupField.VALUE, v);
                if (v == null) {
                    // Arbitrary value to update the doc ID with for nulls
                    docID.update(19);
                } else if (v instanceof String) {
                    byte[] vs = ((String) v).getBytes(StandardCharsets.UTF_8);
                    docID.update(vs, 0, vs.length);
                } else if (v instanceof Long) {
                    docID.update(Numbers.longToBytes((Long)v), 0, 8);
                } else if (v instanceof Double) {
                    docID.update(Numbers.doubleToBytes((Double)v), 0, 8);
                } else {
                    throw new RuntimeException("Encountered value of type [" + v.getClass() + "], which was unable to be processed.");
                }
            } else {
                throw new ElasticsearchException("Could not identify key in agg [" + k + "]");
            }
        });
        return docID;
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
