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

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface CompositeAggregation extends MultiBucketsAggregation {
    interface Bucket extends MultiBucketsAggregation.Bucket {
        Map<String, Object> getKey();
    }

    @Override
    List<? extends CompositeAggregation.Bucket> getBuckets();

    /**
     * Returns the last key in this aggregation. It can be used to retrieve the buckets that are after these values.
     * See {@link CompositeAggregationBuilder#aggregateAfter}.
     */
    Map<String, Object> afterKey();

    static XContentBuilder bucketToXContent(CompositeAggregation.Bucket bucket,
                                            XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        buildCompositeMap(CommonFields.KEY.getPreferredName(), bucket.getKey(), builder);
        builder.field(CommonFields.DOC_COUNT.getPreferredName(), bucket.getDocCount());
        bucket.getAggregations().toXContentInternal(builder, params);
        builder.endObject();
        return builder;
    }

    static XContentBuilder toXContentFragment(CompositeAggregation aggregation, XContentBuilder builder, Params params) throws IOException {
        builder.startArray(CommonFields.BUCKETS.getPreferredName());
        for (CompositeAggregation.Bucket bucket : aggregation.getBuckets()) {
            bucketToXContent(bucket, builder, params);
        }
        builder.endArray();
        return builder;
    }

    static void buildCompositeMap(String fieldName, Map<String, Object> composite, XContentBuilder builder) throws IOException {
        builder.startObject(fieldName);
        for (Map.Entry<String, Object> entry : composite.entrySet()) {
            if (entry.getValue().getClass() == BytesRef.class) {
                builder.field(entry.getKey(), ((BytesRef) entry.getValue()).utf8ToString());
            } else {
                builder.field(entry.getKey(), entry.getValue());
            }
        }
        builder.endObject();
    }
}
