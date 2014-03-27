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
package org.elasticsearch.search.aggregations.bucket.terms;

import com.google.common.primitives.Longs;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.Comparators;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.support.OrderPath;

import java.io.IOException;
import java.util.Comparator;

/**
 *
 */
class InternalOrder extends Terms.Order {

    /**
     * Order by the (higher) count of each term.
     */
    public static final InternalOrder COUNT_DESC = new InternalOrder((byte) 1, "_count", false, new Comparator<Terms.Bucket>() {
        @Override
        public int compare(Terms.Bucket o1, Terms.Bucket o2) {
            int cmp = - Long.compare(o1.getDocCount(), o2.getDocCount());
            if (cmp == 0) {
                cmp = o1.compareTerm(o2);
            }
            return cmp;
        }
    });

    /**
     * Order by the (lower) count of each term.
     */
    public static final InternalOrder COUNT_ASC = new InternalOrder((byte) 2, "_count", true, new Comparator<Terms.Bucket>() {

        @Override
        public int compare(Terms.Bucket o1, Terms.Bucket o2) {
            int cmp = Long.compare(o1.getDocCount(), o2.getDocCount());
            if (cmp == 0) {
                cmp = o1.compareTerm(o2);
            }
            return cmp;
        }
    });

    /**
     * Order by the terms.
     */
    public static final InternalOrder TERM_DESC = new InternalOrder((byte) 3, "_term", false, new Comparator<Terms.Bucket>() {

        @Override
        public int compare(Terms.Bucket o1, Terms.Bucket o2) {
            return - o1.compareTerm(o2);
        }
    });

    /**
     * Order by the terms.
     */
    public static final InternalOrder TERM_ASC = new InternalOrder((byte) 4, "_term", true, new Comparator<Terms.Bucket>() {

        @Override
        public int compare(Terms.Bucket o1, Terms.Bucket o2) {
            return o1.compareTerm(o2);
        }
    });


    final byte id;
    final String key;
    final boolean asc;
    protected final Comparator<Terms.Bucket> comparator;

    InternalOrder(byte id, String key, boolean asc, Comparator<Terms.Bucket> comparator) {
        this.id = id;
        this.key = key;
        this.asc = asc;
        this.comparator = comparator;
    }

    byte id() {
        return id;
    }

    @Override
    protected Comparator<Terms.Bucket> comparator(Aggregator aggregator) {
        return comparator;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field(key, asc ? "asc" : "desc").endObject();
    }

    public static InternalOrder validate(InternalOrder order, Aggregator termsAggregator) {
        if (!(order instanceof Aggregation)) {
            return order;
        }
        OrderPath path = ((Aggregation) order).path();
        path.validate(termsAggregator);
        return order;
    }

    static class Aggregation extends InternalOrder {

        static final byte ID = 0;

        Aggregation(String key, boolean asc) {
            super(ID, key, asc, new MultiBucketsAggregation.Bucket.SubAggregationComparator<Terms.Bucket>(key, asc));
        }

        OrderPath path() {
            return ((MultiBucketsAggregation.Bucket.SubAggregationComparator) comparator).path();
        }

        @Override
        protected Comparator<Terms.Bucket> comparator(Aggregator termsAggregator) {
            if (termsAggregator == null) {
                return comparator;
            }

            // Internal Optimization:
            //
            // in this phase, if the order is based on sub-aggregations, we need to use a different comparator
            // to avoid constructing buckets for ordering purposes (we can potentially have a lot of buckets and building
            // them will cause loads of redundant object constructions). The "special" comparators here will fetch the
            // sub aggregation values directly from the sub aggregators bypassing bucket creation. Note that the comparator
            // attached to the order will still be used in the reduce phase of the Aggregation.

            OrderPath path = path();
            final Aggregator aggregator = path.resolveAggregator(termsAggregator, false);
            final String key = path.tokens[path.tokens.length - 1].key;

            if (aggregator instanceof SingleBucketAggregator) {
                assert key == null : "this should be picked up before the aggregation is executed - on validate";
                return new Comparator<Terms.Bucket>() {
                    @Override
                    public int compare(Terms.Bucket o1, Terms.Bucket o2) {
                        long v1 = ((SingleBucketAggregator) aggregator).bucketDocCount(((InternalTerms.Bucket) o1).bucketOrd);
                        long v2 = ((SingleBucketAggregator) aggregator).bucketDocCount(((InternalTerms.Bucket) o2).bucketOrd);
                        return asc ? Long.compare(v1, v2) : Long.compare(v2, v1);
                    }
                };
            }

            // with only support single-bucket aggregators
            assert !(aggregator instanceof BucketsAggregator) : "this should be picked up before the aggregation is executed - on validate";

            if (aggregator instanceof MetricsAggregator.MultiValue) {
                assert key != null : "this should be picked up before the aggregation is executed - on validate";
                return new Comparator<Terms.Bucket>() {
                    @Override
                    public int compare(Terms.Bucket o1, Terms.Bucket o2) {
                        double v1 = ((MetricsAggregator.MultiValue) aggregator).metric(key, ((InternalTerms.Bucket) o1).bucketOrd);
                        double v2 = ((MetricsAggregator.MultiValue) aggregator).metric(key, ((InternalTerms.Bucket) o2).bucketOrd);
                        // some metrics may return NaN (eg. avg, variance, etc...) in which case we'd like to push all of those to
                        // the bottom
                        return Comparators.compareDiscardNaN(v1, v2, asc);
                    }
                };
            }

            // single-value metrics agg
            return new Comparator<Terms.Bucket>() {
                @Override
                public int compare(Terms.Bucket o1, Terms.Bucket o2) {
                    double v1 = ((MetricsAggregator.SingleValue) aggregator).metric(((InternalTerms.Bucket) o1).bucketOrd);
                    double v2 = ((MetricsAggregator.SingleValue) aggregator).metric(((InternalTerms.Bucket) o2).bucketOrd);
                    // some metrics may return NaN (eg. avg, variance, etc...) in which case we'd like to push all of those to
                    // the bottom
                    return Comparators.compareDiscardNaN(v1, v2, asc);
                }
            };
        }
    }

    public static class Streams {

        public static void writeOrder(InternalOrder order, StreamOutput out) throws IOException {
            out.writeByte(order.id());
            if (order instanceof Aggregation) {
                out.writeBoolean(((MultiBucketsAggregation.Bucket.SubAggregationComparator) order.comparator).asc());
                OrderPath path = ((Aggregation) order).path();
                if (out.getVersion().onOrAfter(Version.V_1_1_0)) {
                    out.writeString(path.toString());
                } else {
                    // prev versions only supported sorting on a single level -> a single token;
                    OrderPath.Token token = path.lastToken();
                    out.writeString(token.name);
                    boolean hasValueName = token.key != null;
                    out.writeBoolean(hasValueName);
                    if (hasValueName) {
                        out.writeString(token.key);
                    }
                }
            }
        }

        public static InternalOrder readOrder(StreamInput in) throws IOException {
            byte id = in.readByte();
            switch (id) {
                case 1: return InternalOrder.COUNT_DESC;
                case 2: return InternalOrder.COUNT_ASC;
                case 3: return InternalOrder.TERM_DESC;
                case 4: return InternalOrder.TERM_ASC;
                case 0:
                    boolean asc = in.readBoolean();
                    String key = in.readString();
                    if (in.getVersion().onOrAfter(Version.V_1_1_0)) {
                        return new InternalOrder.Aggregation(key, asc);
                    }
                    boolean hasValueNmae = in.readBoolean();
                    if (hasValueNmae) {
                        return new InternalOrder.Aggregation(key + "." + in.readString(), asc);
                    }
                    return new InternalOrder.Aggregation(key, asc);
                default:
                    throw new RuntimeException("unknown terms order");
            }
        }
    }
}
