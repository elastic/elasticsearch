/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalTDigestPercentiles;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class BucketHelpersTests extends ESTestCase {

    public void testReturnsObjectArray() {

        MultiBucketsAggregation agg = new MultiBucketsAggregation() {
            @Override
            public List<? extends Bucket> getBuckets() {
                return null;
            }

            @Override
            public String getName() {
                return "foo";
            }

            @Override
            public String getType() {
                return null;
            }

            @Override
            public Map<String, Object> getMetadata() {
                return null;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return null;
            }
        };

        InternalMultiBucketAggregation.InternalBucket bucket = new InternalMultiBucketAggregation.InternalBucket() {

            @Override
            public Object getKey() {
                return null;
            }

            @Override
            public String getKeyAsString() {
                return null;
            }

            @Override
            public long getDocCount() {
                return 0;
            }

            @Override
            public InternalAggregations getAggregations() {
                return null;
            }

            @Override
            public Object getProperty(String containingAggName, List<String> path) {
                return new Object[0];
            }
        };

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> BucketHelpers.resolveBucketValue(agg, bucket, "foo>bar", BucketHelpers.GapPolicy.SKIP)
        );

        assertThat(
            e.getMessage(),
            equalTo(
                "buckets_path must reference either a number value or a single value numeric "
                    + "metric aggregation, got: [Object[]] at aggregation [foo]"
            )
        );
    }

    public void testReturnMultiValueObject() {

        MultiBucketsAggregation agg = new MultiBucketsAggregation() {
            @Override
            public List<? extends Bucket> getBuckets() {
                return null;
            }

            @Override
            public String getName() {
                return "foo";
            }

            @Override
            public String getType() {
                return null;
            }

            @Override
            public Map<String, Object> getMetadata() {
                return null;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return null;
            }
        };

        InternalMultiBucketAggregation.InternalBucket bucket = new InternalMultiBucketAggregation.InternalBucket() {

            @Override
            public Object getKey() {
                return null;
            }

            @Override
            public String getKeyAsString() {
                return null;
            }

            @Override
            public long getDocCount() {
                return 0;
            }

            @Override
            public InternalAggregations getAggregations() {
                return null;
            }

            @Override
            public Object getProperty(String containingAggName, List<String> path) {
                return mock(InternalTDigestPercentiles.class);
            }
        };

        assertEquals(Double.valueOf(0.0), BucketHelpers.resolveBucketValue(agg, bucket, "foo>bar", BucketHelpers.GapPolicy.INSERT_ZEROS));
    }
}
