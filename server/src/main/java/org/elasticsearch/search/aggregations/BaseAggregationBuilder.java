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

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;

import java.util.Map;

/**
 * Interface shared by {@link AggregationBuilder} and {@link PipelineAggregationBuilder} so they can conveniently share the same namespace
 * for {@link XContentParser#namedObject(Class, String, Object)}.
 */
public interface BaseAggregationBuilder {
    /**
     * The name of the type of aggregation built by this builder.
     */
    String getType();

    /**
     * Set the aggregation's metadata. Returns {@code this} for chaining.
     */
    BaseAggregationBuilder setMetadata(Map<String, Object> metadata);

    /**
     * Set the sub aggregations if this aggregation supports sub aggregations. Returns {@code this} for chaining.
     */
    BaseAggregationBuilder subAggregations(Builder subFactories);
}
