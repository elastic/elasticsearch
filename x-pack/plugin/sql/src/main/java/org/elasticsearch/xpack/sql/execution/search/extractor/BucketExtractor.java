/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search.extractor;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;

/**
 * Extracts an aggregation value from a {@link Bucket}.
 */
public interface BucketExtractor extends NamedWriteable {

    Object extract(Bucket bucket);
}
