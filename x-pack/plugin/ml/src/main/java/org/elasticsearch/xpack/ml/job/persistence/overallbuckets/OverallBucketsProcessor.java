/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.persistence.overallbuckets;

import org.elasticsearch.xpack.core.ml.job.results.OverallBucket;

import java.util.List;

public interface OverallBucketsProcessor {

    void process(List<OverallBucket> overallBuckets);

    List<OverallBucket> finish();

    int size();
}
