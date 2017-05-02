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

import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class ParsedMultiBucketAggregation extends ParsedAggregation implements MultiBucketsAggregation {

    protected final List<ParsedBucket<?>> buckets = new ArrayList<>();
    protected boolean keyed;

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (keyed) {
            builder.startObject(CommonFields.BUCKETS.getPreferredName());
        } else {
            builder.startArray(CommonFields.BUCKETS.getPreferredName());
        }
        for (ParsedMultiBucketAggregation.ParsedBucket<?> bucket : buckets) {
            bucket.toXContent(builder, params);
        }
        if (keyed) {
            builder.endObject();
        } else {
            builder.endArray();
        }
        return builder;
    }

    protected static void declareMultiBucketAggregationFields(final ObjectParser<? extends ParsedMultiBucketAggregation, Void> objectParser,
                                                              final CheckedFunction<XContentParser, ParsedBucket<?>, IOException> bucketParser,
                                                              final CheckedFunction<XContentParser, ParsedBucket<?>, IOException> keyedBucketParser) {
        declareAggregationFields(objectParser);
        objectParser.declareField((parser, aggregation, context) -> {
            XContentParser.Token token = parser.currentToken();
            if (token == XContentParser.Token.START_OBJECT) {
                aggregation.keyed = true;
                while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                    aggregation.buckets.add(keyedBucketParser.apply(parser));
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                aggregation.keyed = false;
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    aggregation.buckets.add(bucketParser.apply(parser));
                }
            }
        }, CommonFields.BUCKETS, ObjectParser.ValueType.OBJECT_ARRAY);
    }

    public static class ParsedBucket<T> implements MultiBucketsAggregation.Bucket {

        private List<? extends Aggregation> aggregations = Collections.emptyList();
        private T key;
        private String keyAsString;
        private long docCount;
        private boolean keyed;

        protected void setKey(T key) {
            this.key = key;
        }

        @Override
        public Object getKey() {
            return key;
        }

        protected void setKeyAsString(String keyAsString) {
            this.keyAsString = keyAsString;
        }

        @Override
        public String getKeyAsString() {
            return keyAsString;
        }

        protected void setDocCount(long docCount) {
            this.docCount = docCount;
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        public void setKeyed(boolean keyed) {
            this.keyed = keyed;
        }

        protected void setAggregations(List<? extends Aggregation> aggregations) {
            this.aggregations = aggregations;
        }

        @Override
        public Aggregations getAggregations() {
            return new Aggregations(aggregations) {};
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (keyed) {
                builder.startObject(getKeyAsString());
            } else {
                builder.startObject();
            }
            if (keyAsString != null) {
                builder.field(CommonFields.KEY_AS_STRING.getPreferredName(), keyAsString);
            }
            builder.field(CommonFields.KEY.getPreferredName(), key);
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), docCount);
            for (Aggregation aggregation : aggregations) {
                if (aggregation instanceof ParsedAggregation) {
                    ((ParsedAggregation) aggregation).toXContent(builder, params);
                }
            }
            builder.endObject();
            return builder;
        }
    }
}
