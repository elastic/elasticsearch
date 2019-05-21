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

import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

public class ParsedDateHistogram extends ParsedMultiBucketAggregation<ParsedDateHistogram.ParsedBucket> implements Histogram {

    @Override
    public String getType() {
        return DateHistogramAggregationBuilder.NAME;
    }

    @Override
    public List<? extends Histogram.Bucket> getBuckets() {
        return buckets;
    }

    private static ObjectParser<ParsedDateHistogram, Void> PARSER =
            new ObjectParser<>(ParsedDateHistogram.class.getSimpleName(), true, ParsedDateHistogram::new);
    static {
        declareMultiBucketAggregationFields(PARSER,
                parser -> ParsedBucket.fromXContent(parser, false),
                parser -> ParsedBucket.fromXContent(parser, true));
    }

    public static ParsedDateHistogram fromXContent(XContentParser parser, String name) throws IOException {
        ParsedDateHistogram aggregation = PARSER.parse(parser, null);
        aggregation.setName(name);
        return aggregation;
    }

    public static class ParsedBucket extends ParsedMultiBucketAggregation.ParsedBucket implements Histogram.Bucket {

        private Long key;

        @Override
        public Object getKey() {
            if (key != null) {
                return Instant.ofEpochMilli(key).atZone(ZoneOffset.UTC);
            }
            return null;
        }

        @Override
        public String getKeyAsString() {
            String keyAsString = super.getKeyAsString();
            if (keyAsString != null) {
                return keyAsString;
            }
            if (key != null) {
                return Long.toString(key);
            }
            return null;
        }

        @Override
        protected XContentBuilder keyToXContent(XContentBuilder builder) throws IOException {
            return builder.field(CommonFields.KEY.getPreferredName(), key);
        }

        static ParsedBucket fromXContent(XContentParser parser, boolean keyed) throws IOException {
            return parseXContent(parser, keyed, ParsedBucket::new, (p, bucket) -> bucket.key = p.longValue());
        }
    }
}
