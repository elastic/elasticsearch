/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.composite;

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
        if (aggregation.afterKey() != null) {
            buildCompositeMap("after_key", aggregation.afterKey(), builder);
        }
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
            builder.field(entry.getKey(), entry.getValue());
        }
        builder.endObject();
    }
}
