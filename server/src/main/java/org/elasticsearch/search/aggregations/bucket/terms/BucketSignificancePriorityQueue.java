/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.util.PriorityQueue;

public class BucketSignificancePriorityQueue<B extends SignificantTerms.Bucket> extends PriorityQueue<B> {

    public BucketSignificancePriorityQueue(int size) {
        super(size);
    }

    @Override
    protected boolean lessThan(SignificantTerms.Bucket o1, SignificantTerms.Bucket o2) {
        return o1.getSignificanceScore() < o2.getSignificanceScore();
    }
}
