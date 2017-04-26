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
package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
/**
 * An internal implementation of {@link Aggregations}.
 */
public final class ParsedAggregations extends Aggregations implements ToXContent {

    /**
     * Constructs a new addAggregation.
     */
    public ParsedAggregations(List<Aggregation> aggregations) {
        super(aggregations);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (aggregations.isEmpty()) {
            return builder;
        }
        builder.startObject("aggregations");
        toXContentInternal(builder, params);
        return builder.endObject();
    }

    /**
     * Directly write all the aggregations without their bounding object. Used by sub-aggregations (non top level aggs)
     */
    public XContentBuilder toXContentInternal(XContentBuilder builder, Params params) throws IOException {
        for (Aggregation aggregation : aggregations) {
            ((ParsedAggregation) aggregation).toXContent(builder, params);
        }
        return builder;
    }
}
