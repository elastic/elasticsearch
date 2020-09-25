/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.mapper.fielddata;


import org.elasticsearch.search.aggregations.metrics.AbstractHyperLogLog.RunLenIterator;
import org.elasticsearch.search.aggregations.metrics.AbstractLinearCounting.EncodedHashesIterator;

/**
 * Per-document HyperLogLogPlusPlus value.
 */
public abstract class HyperLogLogPlusPlusValue {

    public enum Algorithm {LINEAR_COUNTING, HYPERLOGLOG};

    /** Algorithm for this value */
    public abstract Algorithm getAlgorithm();

    /** Get linear counting algorithm */
    public abstract EncodedHashesIterator getLinearCounting();

    /** Get HyperLogLog algorithm */
    public abstract RunLenIterator getHyperLogLog();
}
