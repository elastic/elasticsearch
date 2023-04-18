/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.terms.heuristic;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.bucket.terms.SignificantTerms;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.xcontent.ToXContentFragment;

/**
 * Heuristic for that {@link SignificantTerms} uses to pick out significant terms.
 */
public abstract class SignificanceHeuristic implements NamedWriteable, ToXContentFragment {
    /**
     * @param subsetFreq   The frequency of the term in the selected sample
     * @param subsetSize   The size of the selected sample (typically number of docs)
     * @param supersetFreq The frequency of the term in the superset from which the sample was taken
     * @param supersetSize The size of the superset from which the sample was taken  (typically number of docs)
     * @return a "significance" score
     */
    public abstract double getScore(long subsetFreq, long subsetSize, long supersetFreq, long supersetSize);

    protected static void checkFrequencyValidity(
        long subsetFreq,
        long subsetSize,
        long supersetFreq,
        long supersetSize,
        String scoreFunctionName
    ) {
        if (subsetFreq < 0 || subsetSize < 0 || supersetFreq < 0 || supersetSize < 0) {
            throw new IllegalArgumentException(
                "Frequencies of subset and superset must be positive in " + scoreFunctionName + ".getScore()"
            );
        }
        if (subsetFreq > subsetSize) {
            throw new IllegalArgumentException("subsetFreq > subsetSize, in " + scoreFunctionName);
        }
        if (supersetFreq > supersetSize) {
            throw new IllegalArgumentException("supersetFreq > supersetSize, in " + scoreFunctionName);
        }
    }

    /**
     * Provides a hook for subclasses to provide a version of the heuristic
     * prepared for execution on data on the coordinating node.
     * @param reduceContext the reduce context on the coordinating node
     * @return a version of this heuristic suitable for execution
     */
    public SignificanceHeuristic rewrite(AggregationReduceContext reduceContext) {
        return this;
    }

    /**
     * Provides a hook for subclasses to provide a version of the heuristic
     * prepared for execution on data on a shard.
     * @param context the shard context on the data node
     * @return a version of this heuristic suitable for execution
     */
    public SignificanceHeuristic rewrite(AggregationContext context) {
        return this;
    }
}
