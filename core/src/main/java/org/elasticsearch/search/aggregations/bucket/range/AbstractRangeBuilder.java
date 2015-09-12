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

package org.elasticsearch.search.aggregations.bucket.range;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilderException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public abstract class AbstractRangeBuilder<B extends AbstractRangeBuilder<B>> extends ValuesSourceAggregationBuilder<B> {

    protected static class Range implements ToXContent {

        private String key;
        private Object from;
        private Object to;

        public Range(String key, Object from, Object to) {
            this.key = key;
            this.from = from;
            this.to = to;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (key != null) {
                builder.field("key", key);
            }
            if (from != null) {
                builder.field("from", from);
            }
            if (to != null) {
                builder.field("to", to);
            }
            return builder.endObject();
        }
    }

    protected List<Range> ranges = new ArrayList<>();

    protected AbstractRangeBuilder(String name, String type) {
        super(name, type);
    }

    @Override
    protected XContentBuilder doInternalXContent(XContentBuilder builder, Params params) throws IOException {
        if (ranges.isEmpty()) {
            throw new SearchSourceBuilderException("at least one range must be defined for range aggregation [" + getName() + "]");
        }
        builder.startArray("ranges");
        for (Range range : ranges) {
            range.toXContent(builder, params);
        }
        return builder.endArray();
    }
}
