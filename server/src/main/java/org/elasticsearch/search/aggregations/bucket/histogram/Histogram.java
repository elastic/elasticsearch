/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.xcontent.ParseField;

/**
 * A {@code histogram} aggregation. Defines multiple buckets, each representing an interval in a histogram.
 */
public interface Histogram extends MultiBucketsAggregation {

    ParseField INTERVAL_FIELD = new ParseField("interval");
    ParseField OFFSET_FIELD = new ParseField("offset");
    ParseField ORDER_FIELD = new ParseField("order");
    ParseField KEYED_FIELD = new ParseField("keyed");
    ParseField MIN_DOC_COUNT_FIELD = new ParseField("min_doc_count");
    ParseField EXTENDED_BOUNDS_FIELD = new ParseField("extended_bounds");
    ParseField HARD_BOUNDS_FIELD = new ParseField("hard_bounds");

}
