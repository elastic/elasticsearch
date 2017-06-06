/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.util.List;

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

    /**
     * A bucket in the histogram where documents fall in
     */
    interface Bucket extends MultiBucketsAggregation.Bucket {

    }

    /**
     * @return  The buckets of this histogram (each bucket representing an interval in the histogram)
     */
    @Override
    List<? extends Bucket> getBuckets();

}
