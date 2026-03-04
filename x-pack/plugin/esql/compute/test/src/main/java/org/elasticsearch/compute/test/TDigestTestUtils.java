/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.test;

import org.elasticsearch.compute.aggregation.TDigestStates;
import org.elasticsearch.compute.data.TDigestHolder;
import org.elasticsearch.search.aggregations.metrics.TDigestState;
import org.elasticsearch.tdigest.Centroid;

import java.util.Collection;

public class TDigestTestUtils {

    /**
     * Utility method for verifying that a TDigestHolder is a correct merge of a collection of TDigestHolders.
     * TDigest is non-deterministic, we just do a sanity check here:
     * the total count, min and max should match exactly and the sum should be close (1% error allowed).
     * In addition, we check the p1 and p99 with a rather large tolerance.
     */
    public static boolean isMergedFrom(TDigestHolder merged, Collection<TDigestHolder> inputValues) {
        long totalCount = 0;
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        double sum = 0.0;
        boolean anyValuesNonNull = false;

        TDigestState reference = TDigestState.createWithoutCircuitBreaking(TDigestStates.COMPRESSION);

        for (var tdigest : inputValues) {
            if (tdigest != null) {
                anyValuesNonNull = true;
                totalCount += tdigest.getValueCount();
                min = Double.isNaN(tdigest.getMin()) ? min : Math.min(min, tdigest.getMin());
                max = Double.isNaN(tdigest.getMax()) ? max : Math.max(max, tdigest.getMax());
                sum += Double.isNaN(tdigest.getSum()) ? 0.0 : tdigest.getSum();

                TDigestState decoded = TDigestState.createWithoutCircuitBreaking(TDigestStates.COMPRESSION);
                tdigest.addTo(decoded);
                tdigest.addTo(reference);
            }
        }

        if (anyValuesNonNull == false) {
            return merged == null;
        }

        if (totalCount > 0) {
            double sumError;
            if (Math.abs(sum) < 0.0001) {
                // close to zero, use absolute error
                sumError = Math.abs(sum - merged.getSum());
            } else {
                // otherwise relative error
                sumError = Math.abs(1 - merged.getSum() / sum);
            }
            if (sumError > 0.01) {
                return false;
            }
            if (min != merged.getMin() || max != merged.getMax()) {
                return false;
            }
        } else {
            if (Double.isNaN(merged.getMin()) == false
                || Double.isNaN(merged.getMax()) == false
                || Double.isNaN(merged.getSum()) == false) {
                return false;
            }
        }
        if (totalCount != merged.getValueCount()) {
            return false;
        }

        TDigestState decoded = TDigestState.createWithoutCircuitBreaking(TDigestStates.COMPRESSION);
        merged.addTo(decoded);
        long tDigestTotalCount = 0;
        for (Centroid centroid : decoded.centroids()) {
            tDigestTotalCount += centroid.count();
        }
        if (tDigestTotalCount != totalCount) {
            return false;
        }
        if (tDigestTotalCount > 0) {
            if (Math.abs(decoded.quantile(0.01) - reference.quantile(0.01)) > 0.1) {
                return false;
            }
            if (Math.abs(decoded.quantile(0.99) - reference.quantile(0.99)) > 0.1) {
                return false;
            }
        }

        return true;
    }
}
