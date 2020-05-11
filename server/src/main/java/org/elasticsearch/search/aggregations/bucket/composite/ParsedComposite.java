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

package org.elasticsearch.search.aggregations.bucket.composite;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ParsedComposite extends ParsedMultiBucketAggregation<ParsedComposite.ParsedBucket> implements CompositeAggregation {
    private static final ObjectParser<ParsedComposite, Void> PARSER =
        new ObjectParser<>(ParsedComposite.class.getSimpleName(), true, ParsedComposite::new);

    static {
        PARSER.declareField(ParsedComposite::setAfterKey, (p, c) -> p.mapOrdered(), new ParseField("after_key"),
            ObjectParser.ValueType.OBJECT);
        declareMultiBucketAggregationFields(PARSER,
            parser -> ParsedComposite.ParsedBucket.fromXContent(parser),
            parser -> null
        );
    }

    private Map<String, Object> afterKey;

    public static ParsedComposite fromXContent(XContentParser parser, String name) throws IOException {
        ParsedComposite aggregation = PARSER.parse(parser, null);
        aggregation.setName(name);
        if (aggregation.afterKey == null && aggregation.getBuckets().size() > 0) {
            /**
             * Previous versions (< 6.3) don't send <code>afterKey</code>
             * in the response so we set it as the last returned buckets.
             */
            aggregation.setAfterKey(aggregation.getBuckets().get(aggregation.getBuckets().size()-1).key);
        }
        return aggregation;
    }

    @Override
    public String getType() {
        return CompositeAggregationBuilder.NAME;
    }

    @Override
    public List<ParsedBucket> getBuckets() {
        return buckets;
    }

    @Override
    public Map<String, Object> afterKey() {
        if (afterKey != null) {
            return afterKey;
        }
        return buckets.size() > 0 ? buckets.get(buckets.size()-1).getKey() : null;
    }

    private void setAfterKey(Map<String, Object> afterKey) {
        this.afterKey = afterKey;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return CompositeAggregation.toXContentFragment(this, builder, params);
    }

    public static class ParsedBucket extends ParsedMultiBucketAggregation.ParsedBucket implements CompositeAggregation.Bucket {
        private Map<String, Object> key;

        @Override
        public String getKeyAsString() {
            return key.toString();
        }

        @Override
        public Map<String, Object> getKey() {
            return key;
        }

        void setKey(Map<String, Object> key) {
            this.key = key;
        }

        @Override
        public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            /**
             * See {@link CompositeAggregation#bucketToXContent}
             */
            throw new UnsupportedOperationException("not implemented");
        }

        static ParsedComposite.ParsedBucket fromXContent(XContentParser parser) throws IOException {
            return parseXContent(parser, false, ParsedBucket::new,
                (p, bucket) -> bucket.setKey(p.mapOrdered()));
        }
    }
}
