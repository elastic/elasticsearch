/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalMultiValueAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalTDigestPercentiles;
import org.elasticsearch.search.sort.SortValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class BucketHelpersTests extends ESTestCase {

    private static MultiBucketsAggregation newDummyAggregation() {
        return new MultiBucketsAggregation() {
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
    }

    public void testReturnsObjectArray() {

        MultiBucketsAggregation agg = newDummyAggregation();

        InternalMultiBucketAggregation.InternalBucket bucket = new InternalMultiBucketAggregation.InternalBucket() {
            @Override
            public void writeTo(StreamOutput out) throws IOException {}

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
            public Aggregations getAggregations() {
                return null;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return null;
            }

            @Override
            public Object getProperty(String containingAggName, List<String> path) {
                return new Object[0];
            }
        };

        AggregationExecutionException e = expectThrows(
            AggregationExecutionException.class,
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

        MultiBucketsAggregation agg = newDummyAggregation();

        InternalMultiBucketAggregation.InternalBucket bucket = new InternalMultiBucketAggregation.InternalBucket() {
            @Override
            public void writeTo(StreamOutput out) throws IOException {}

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
            public Aggregations getAggregations() {
                return null;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return null;
            }

            @Override
            public Object getProperty(String containingAggName, List<String> path) {
                return mock(InternalTDigestPercentiles.class);
            }
        };
        assertEquals(Double.valueOf(0.0), BucketHelpers.resolveBucketValue(agg, bucket, "foo>bar", BucketHelpers.GapPolicy.INSERT_ZEROS));
    }

    public void testReturnValueFromInternalAggregation() {

        MultiBucketsAggregation agg = newDummyAggregation();

        InternalMultiBucketAggregation.InternalBucket bucket = new InternalMultiBucketAggregation.InternalBucket() {
            private final InternalMultiValueAggregation internalAggregation = new InternalMultiValueAggregation("internal", null) {
                @Override
                public Iterable<String> valueNames() {
                    return null;
                }

                @Override
                public List<String> getValuesAsStrings(String name) {
                    return null;
                }

                @Override
                protected void doWriteTo(StreamOutput out) throws IOException {}

                @Override
                public InternalAggregation reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
                    return null;
                }

                @Override
                protected boolean mustReduceOnSingleInternalAgg() {
                    return false;
                }

                @Override
                public Object getProperty(List<String> path) {
                    return new Object[0];
                }

                @Override
                public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
                    return null;
                }

                @Override
                public String getWriteableName() {
                    return null;
                }

                @Override
                public SortValue sortValue(String key) {
                    return SortValue.from(10);
                }
            };

            @Override
            public void writeTo(StreamOutput out) throws IOException {}

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
            public Aggregations getAggregations() {
                return new Aggregations(List.of(internalAggregation));
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return null;
            }

            @Override
            public Object getProperty(String containingAggName, List<String> path) {
                return internalAggregation;
            }
        };

        assertEquals(Double.valueOf(10.0), BucketHelpers.resolveBucketValue(agg, bucket, "foo>bar", BucketHelpers.GapPolicy.KEEP_VALUES));
    }
}
