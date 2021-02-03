/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.global;

import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;

/**
 * A {@code global} aggregation. Defines a single bucket the holds all the documents in the search context.
 */
public interface Global extends SingleBucketAggregation {
}
