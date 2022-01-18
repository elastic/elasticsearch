/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.sampler;

import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;

/**
 * A {@code filter} aggregation that defines a single bucket to hold a sample of
 * top-matching documents. Computation of child aggregations is deferred until
 * the top-matching documents on a shard have been determined.
 */
public interface Sampler extends SingleBucketAggregation {}
