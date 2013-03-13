/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.sort;

import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * A builder for random sort
 */
public class RandomSortBuilder extends SortBuilder {

    private SortOrder order;
    private long seed;

    /**
     * Creates this builder with the current timestamp as the default seed for the RNG.
     */
    public RandomSortBuilder() {
        this(System.currentTimeMillis());
    }

    /**
     * Creates this builder with a seed to base the random number generation on
     *
     * @param seed The seed
     */
    public RandomSortBuilder(long seed) {
        this.seed = seed;
    }

    /**
     * Sets the sort order
     *
     * @param order The sort order
     */
    @Override
    public RandomSortBuilder order(SortOrder order) {
        this.order = order;
        return this;
    }

    /**
     * @return The seed that the RNG will be based on
     */
    public long seed() {
        return seed;
    }

    /**
     * Sets the seed that the RNG will be based on
     *
     * @param seed The seed that the RNG will be based on
     */
    public RandomSortBuilder seed(long seed) {
        this.seed = seed;
        return this;
    }

    @Override
    public SortBuilder missing(Object missing) {
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("_random");
        if (order == SortOrder.ASC) {
            builder.field("reverse", true);
        }
        builder.field("seed", seed);
        builder.endObject();
        return builder;
    }
}
