/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.bucket.Bucket;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;

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
            int cmp = - Longs.compare(o1.getDocCount(), o2.getDocCount());
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
            int cmp = Longs.compare(o1.getDocCount(), o2.getDocCount());
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

    String key() {
        return key;
    }

    boolean asc() {
        return asc;
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
        String aggName = ((Aggregation) order).aggName();
        Aggregator[] subAggregators = termsAggregator.subAggregators();
        for (int i = 0; i < subAggregators.length; i++) {
            Aggregator aggregator = subAggregators[i];
            if (aggregator.name().equals(aggName)) {

                // we can only apply order on metrics sub-aggregators
                if (!(aggregator instanceof MetricsAggregator)) {
                    throw new AggregationExecutionException("terms aggregation [" + termsAggregator.name() + "] is configured to order by sub-aggregation ["
                            + aggName + "] which is is not a metrics aggregation. Terms aggregation order can only refer to metrics aggregations");
                }

                if (aggregator instanceof MetricsAggregator.MultiValue) {
                    String valueName = ((Aggregation) order).metricName();
                    if (valueName == null) {
                        throw new AggregationExecutionException("terms aggregation [" + termsAggregator.name() + "] is configured with a sub-aggregation order ["
                                + aggName + "] which is a multi-valued aggregation, yet no metric name was specified");
                    }
                    if (!((MetricsAggregator.MultiValue) aggregator).hasMetric(valueName)) {
                        throw new AggregationExecutionException("terms aggregation [" + termsAggregator.name() + "] is configured with a sub-aggregation order ["
                                + aggName + "] and value [" + valueName + "] yet the referred sub aggregator holds no metric that goes by this name");
                    }
                    return order;
                }

                // aggregator must be of a single value type
                // todo we can also choose to be really strict and verify that the user didn't specify a value name and if so fail?
                return order;
            }
        }

        throw new AggregationExecutionException("terms aggregation [" + termsAggregator.name() + "] is configured with a sub-aggregation order ["
                + aggName + "] but no sub aggregation with this name is configured");
    }

    static class Aggregation extends InternalOrder {

        static final byte ID = 0;

        Aggregation(String key, boolean asc) {
            super(ID, key, asc, new Bucket.SubAggregationComparator<Terms.Bucket>(key, asc));
        }

        Aggregation(String aggName, String metricName, boolean asc) {
            super(ID, key(aggName, metricName), asc, new Bucket.SubAggregationComparator<Terms.Bucket>(aggName, metricName, asc));
        }

        String aggName() {
            int index = key.indexOf('.');
            return index < 0 ? key : key.substring(0, index);
        }

        String metricName() {
            int index = key.indexOf('.');
            return index < 0 ? null : key.substring(index + 1, key.length());
        }

        private static String key(String aggName, String valueName) {
            return (valueName == null) ? aggName : aggName + "." + valueName;
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

            final Aggregator aggregator = subAggregator(aggName(), termsAggregator);
            assert aggregator != null && aggregator instanceof MetricsAggregator : "this should be picked up before the aggregation is executed";
            if (aggregator instanceof MetricsAggregator.MultiValue) {
                final String valueName = metricName();
                assert valueName != null : "this should be picked up before the aggregation is executed";
                return new Comparator<Terms.Bucket>() {
                    @Override
                    public int compare(Terms.Bucket o1, Terms.Bucket o2) {
                        double v1 = ((MetricsAggregator.MultiValue) aggregator).metric(valueName, ((InternalTerms.Bucket) o1).bucketOrd);
                        double v2 = ((MetricsAggregator.MultiValue) aggregator).metric(valueName, ((InternalTerms.Bucket) o2).bucketOrd);
                        // some metrics may return NaN (eg. avg, variance, etc...) in which case we'd like to push all of those to
                        // the bottom
                        if (v1 == Double.NaN) {
                            return asc ? 1 : -1;
                        }
                        return Double.compare(v1, v2);
                    }
                };
            }

            return new Comparator<Terms.Bucket>() {
                @Override
                public int compare(Terms.Bucket o1, Terms.Bucket o2) {
                    double v1 = ((MetricsAggregator.SingleValue) aggregator).metric(((InternalTerms.Bucket) o1).bucketOrd);
                    double v2 = ((MetricsAggregator.SingleValue) aggregator).metric(((InternalTerms.Bucket) o2).bucketOrd);
                    // some metrics may return NaN (eg. avg, variance, etc...) in which case we'd like to push all of those to
                    // the bottom
                    if (v1 == Double.NaN) {
                        return asc ? 1 : -1;
                    }
                    return Double.compare(v1, v2);
                }
            };
        }

        private Aggregator subAggregator(String aggName, Aggregator termsAggregator) {
            Aggregator[] subAggregators = termsAggregator.subAggregators();
            for (int i = 0; i < subAggregators.length; i++) {
                if (subAggregators[i].name().equals(aggName)) {
                    return subAggregators[i];
                }
            }
            return null;
        }
    }

    public static class Streams {

        public static void writeOrder(InternalOrder order, StreamOutput out) throws IOException {
            out.writeByte(order.id());
            if (order instanceof Aggregation) {
                out.writeBoolean(((Bucket.SubAggregationComparator) order.comparator).asc());
                out.writeString(((Bucket.SubAggregationComparator) order.comparator).aggName());
                boolean hasValueName = ((Bucket.SubAggregationComparator) order.comparator).valueName() != null;
                out.writeBoolean(hasValueName);
                if (hasValueName) {
                    out.writeString(((Bucket.SubAggregationComparator) order.comparator).valueName());
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
                    if (in.readBoolean()) {
                        return new InternalOrder.Aggregation(key, in.readString(), asc);
                    }
                    return new InternalOrder.Aggregation(key, asc);
                default:
                    throw new RuntimeException("unknown terms order");
            }
        }
    }
}
