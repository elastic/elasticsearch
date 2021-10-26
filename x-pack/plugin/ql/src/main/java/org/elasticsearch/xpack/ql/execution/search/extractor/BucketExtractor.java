/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.execution.search.extractor;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;

/**
 * Extracts an aggregation value from a {@link Bucket}.
 */
public interface BucketExtractor extends NamedWriteable {

    Object extract(Bucket bucket);
}
