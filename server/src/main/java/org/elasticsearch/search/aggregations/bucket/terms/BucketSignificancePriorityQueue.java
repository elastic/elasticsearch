/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArrayPriorityQueue;

public class BucketSignificancePriorityQueue<B extends SignificantTerms.Bucket> extends ObjectArrayPriorityQueue<B> {

    public BucketSignificancePriorityQueue(int size, BigArrays bigArrays) {
        super(size, bigArrays);
    }

    @Override
    protected boolean lessThan(SignificantTerms.Bucket o1, SignificantTerms.Bucket o2) {
        return o1.getSignificanceScore() < o2.getSignificanceScore();
    }
}
