/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.stats;

import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class CountAccumulator {

    private final Map<String, Long> counts;

    public CountAccumulator() {
        this.counts = new HashMap<String, Long>();
    }

    private CountAccumulator(Map<String, Long> counts) {
        this.counts = counts;
    }

    public Map<String, Long> asMap() {
        return counts;
    }

    public static CountAccumulator fromTermsAggregation(StringTerms termsAggregation) {
        return new CountAccumulator(termsAggregation.getBuckets().stream()
                .collect(Collectors.toMap(bucket -> bucket.getKeyAsString(), bucket -> bucket.getDocCount())));
    }

}
