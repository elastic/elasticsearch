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

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.search.aggregations.bucket.sampler.SamplerAggregationBuilder;

public class SamplerTests extends BaseAggregationTestCase<SamplerAggregationBuilder> {

    @Override
    protected final SamplerAggregationBuilder createTestAggregatorBuilder() {
        SamplerAggregationBuilder factory = new SamplerAggregationBuilder("foo");
        if (randomBoolean()) {
            factory.shardSize(randomIntBetween(1, 1000));
        }
        return factory;
    }

}
