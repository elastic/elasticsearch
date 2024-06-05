/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.nested;

import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;

/**
 * A {@code nested} aggregation. Defines a single bucket that holds all the nested documents of a specific path.
 */
public interface Nested extends SingleBucketAggregation {}
