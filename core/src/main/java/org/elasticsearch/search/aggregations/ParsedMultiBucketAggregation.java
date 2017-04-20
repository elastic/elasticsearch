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

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public abstract class ParsedMultiBucketAggregation extends ParsedAggregation implements MultiBucketsAggregation {

    public static class ParsedBucket<T> implements MultiBucketsAggregation.Bucket {

        private List<? extends Aggregation> aggregations = Collections.emptyList();
        private T key;
        private String keyAsString;
        private long docCount;
        private String keyedString;

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            //norelease MultiBucketsAggregation.Bucket must not extend Writeable in core - fix that
        }
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
            if (keyAsString != null) {
                return keyAsString;
            } else if (keyedString != null) {
                return keyedString;
            } else {
                return String.valueOf(key);
            }
        }

        protected void setDocCount(long docCount) {
            this.docCount = docCount;
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        protected void setKeyedString(String keyedString) {
            this.keyedString = keyedString;
        }

        protected void setAggregations(List<? extends Aggregation> aggregations) {
            this.aggregations = aggregations;
        }

        @Override
        public Aggregations getAggregations() {
            //norelease use Aggregations abstract class once it is available (#24184)
            return new Aggregations() {

                @Override
                public List<Aggregation> asList() {
                    return Collections.unmodifiableList(new ArrayList<>(aggregations));
                }

                @Override
                public Map<String, Aggregation> asMap() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public Map<String, Aggregation> getAsMap() {
                    Map<String, Aggregation> map = new HashMap<>(aggregations.size());
                    for (Aggregation aggregation : aggregations) {
                        map.put(aggregation.getName(), aggregation);
                    }
                    return map;
                }

                @Override
                public <A extends Aggregation> A get(String name) {
                    return (A) getAsMap().get(name);
                }

                @Override
                public Iterator<Aggregation> iterator() {
                    return asList().iterator();
                }
            };
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (keyedString != null) {
                builder.startObject(keyedString);
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
