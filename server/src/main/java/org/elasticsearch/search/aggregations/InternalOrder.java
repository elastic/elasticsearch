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

import org.elasticsearch.Version;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.Comparators;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationPath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

/**
 * Implementations for {@link Bucket} ordering strategies.
 */
public class InternalOrder extends BucketOrder {

    private final byte id;
    private final String key;
    protected final boolean asc;
    protected final Comparator<Bucket> comparator;

    /**
     * Creates an ordering strategy that sorts {@link Bucket}s by some property.
     *
     * @param id         unique ID for this ordering strategy.
     * @param key        key of the property to sort on.
     * @param asc        direction to sort by: {@code true} for ascending, {@code false} for descending.
     * @param comparator determines how buckets will be ordered.
     */
    public InternalOrder(byte id, String key, boolean asc, Comparator<Bucket> comparator) {
        this.id = id;
        this.key = key;
        this.asc = asc;
        this.comparator = comparator;
    }

    @Override
    byte id() {
        return id;
    }

    @Override
    public Comparator<Bucket> comparator(Aggregator aggregator) {
        return comparator;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field(key, asc ? "asc" : "desc").endObject();
    }

    /**
     * Validate a bucket ordering strategy for an {@link Aggregator}.
     *
     * @param order      bucket ordering strategy to sort on.
     * @param aggregator aggregator to sort.
     * @return unmodified bucket ordering strategy.
     * @throws AggregationExecutionException if validation fails
     */
    public static BucketOrder validate(BucketOrder order, Aggregator aggregator) throws AggregationExecutionException {
        if (order instanceof CompoundOrder) {
            for (BucketOrder innerOrder : ((CompoundOrder) order).orderElements) {
                validate(innerOrder, aggregator);
            }
        } else if (order instanceof Aggregation) {
            ((Aggregation) order).path().validate(aggregator);
        }
        return order;
    }

    /**
     * {@link Bucket} ordering strategy to sort by a sub-aggregation.
     */
    public static class Aggregation extends InternalOrder {

        static final byte ID = 0;

        /**
         * Create a new ordering strategy to sort by a sub-aggregation.
         *
         * @param path path to the sub-aggregation to sort on.
         * @param asc  direction to sort by: {@code true} for ascending, {@code false} for descending.
         * @see AggregationPath
         */
        Aggregation(String path, boolean asc) {
            super(ID, path, asc, new AggregationComparator(path, asc));
        }

        /**
         * @return parsed path to the sub-aggregation to sort on.
         */
        public AggregationPath path() {
            return ((AggregationComparator) comparator).path;
        }

        @Override
        public Comparator<Bucket> comparator(Aggregator aggregator) {
            if (aggregator instanceof TermsAggregator) {
                // Internal Optimization for terms aggregation to avoid constructing buckets for ordering purposes
                return ((TermsAggregator) aggregator).bucketComparator(path(), asc);
            }
            return comparator;
        }

        /**
         * {@link Bucket} ordering strategy to sort by a sub-aggregation.
         */
        static class AggregationComparator implements Comparator<Bucket> {

            private final AggregationPath path;
            private final boolean asc;

            /**
             * Create a new {@link Bucket} ordering strategy to sort by a sub-aggregation.
             *
             * @param path path to the sub-aggregation to sort on.
             * @param asc  direction to sort by: {@code true} for ascending, {@code false} for descending.
             * @see AggregationPath
             */
            AggregationComparator(String path, boolean asc) {
                this.asc = asc;
                this.path = AggregationPath.parse(path);
            }

            @Override
            public int compare(Bucket b1, Bucket b2) {
                double v1 = path.resolveValue(b1);
                double v2 = path.resolveValue(b2);
                return Comparators.compareDiscardNaN(v1, v2, asc);
            }
        }
    }

    /**
     * {@link Bucket} ordering strategy to sort by multiple criteria.
     */
    public static class CompoundOrder extends BucketOrder {

        static final byte ID = -1;

        final List<BucketOrder> orderElements;

        /**
         * Create a new ordering strategy to sort by multiple criteria. A tie-breaker may be added to avoid
         * non-deterministic ordering.
         *
         * @param compoundOrder a list of {@link BucketOrder}s to sort on, in order of priority.
         */
        CompoundOrder(List<BucketOrder> compoundOrder) {
            this(compoundOrder, true);
        }

        /**
         * Create a new ordering strategy to sort by multiple criteria.
         *
         * @param compoundOrder    a list of {@link BucketOrder}s to sort on, in order of priority.
         * @param absoluteOrdering {@code true} to add a tie-breaker to avoid non-deterministic ordering if needed,
         *                         {@code false} otherwise.
         */
        CompoundOrder(List<BucketOrder> compoundOrder, boolean absoluteOrdering) {
            this.orderElements = new LinkedList<>(compoundOrder);
            BucketOrder lastElement = null;
            for (BucketOrder order : orderElements) {
                if (order instanceof CompoundOrder) {
                    throw new IllegalArgumentException("nested compound order not supported");
                }
                lastElement = order;
            }
            if (absoluteOrdering && isKeyOrder(lastElement) == false) {
                // add key order ascending as a tie-breaker to avoid non-deterministic ordering
                // if all user provided comparators return 0.
                this.orderElements.add(KEY_ASC);
            }
        }

        @Override
        byte id() {
            return ID;
        }

        /**
         * @return unmodifiable list of {@link BucketOrder}s to sort on.
         */
        public List<BucketOrder> orderElements() {
            return Collections.unmodifiableList(orderElements);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startArray();
            for (BucketOrder order : orderElements) {
                order.toXContent(builder, params);
            }
            return builder.endArray();
        }

        @Override
        public Comparator<Bucket> comparator(Aggregator aggregator) {
            return new CompoundOrderComparator(orderElements, aggregator);
        }

        @Override
        public int hashCode() {
            return Objects.hash(orderElements);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            CompoundOrder other = (CompoundOrder) obj;
            return Objects.equals(orderElements, other.orderElements);
        }

        /**
         * {@code Comparator} for sorting buckets by multiple criteria.
         */
        static class CompoundOrderComparator implements Comparator<Bucket> {

            private List<BucketOrder> compoundOrder;
            private Aggregator aggregator;

            /**
             * Create a new {@code Comparator} for sorting buckets by multiple criteria.
             *
             * @param compoundOrder a list of {@link BucketOrder}s to sort on, in order of priority.
             * @param aggregator    {@link BucketOrder#comparator(Aggregator)}
             */
            CompoundOrderComparator(List<BucketOrder> compoundOrder, Aggregator aggregator) {
                this.compoundOrder = compoundOrder;
                this.aggregator = aggregator;
            }

            @Override
            public int compare(Bucket b1, Bucket b2) {
                int result = 0;
                for (Iterator<BucketOrder> itr = compoundOrder.iterator(); itr.hasNext() && result == 0; ) {
                    result = itr.next().comparator(aggregator).compare(b1, b2);
                }
                return result;
            }
        }
    }

    private static final byte COUNT_DESC_ID = 1;
    private static final byte COUNT_ASC_ID = 2;
    private static final byte KEY_DESC_ID = 3;
    private static final byte KEY_ASC_ID = 4;

    /**
     * Order by the (higher) count of each bucket.
     */
    static final InternalOrder COUNT_DESC = new InternalOrder(COUNT_DESC_ID, "_count", false, comparingCounts().reversed());

    /**
     * Order by the (lower) count of each bucket.
     */
    static final InternalOrder COUNT_ASC = new InternalOrder(COUNT_ASC_ID, "_count", true, comparingCounts());

    /**
     * Order by the key of each bucket descending.
     */
    static final InternalOrder KEY_DESC = new InternalOrder(KEY_DESC_ID, "_key", false, comparingKeys().reversed());

    /**
     * Order by the key of each bucket ascending.
     */
    static final InternalOrder KEY_ASC = new InternalOrder(KEY_ASC_ID, "_key", true, comparingKeys());

    /**
     * @return compare by {@link Bucket#getDocCount()}.
     */
    private static Comparator<Bucket> comparingCounts() {
        return Comparator.comparingLong(Bucket::getDocCount);
    }

    /**
     * @return compare by {@link Bucket#getKey()} from the appropriate implementation.
     */
    @SuppressWarnings("unchecked")
    private static Comparator<Bucket> comparingKeys() {
        return (b1, b2) -> {
            if (b1 instanceof KeyComparable) {
                return ((KeyComparable) b1).compareKey(b2);
            }
            throw new IllegalStateException("Unexpected order bucket class [" + b1.getClass() + "]");
        };
    }

    /**
     * Determine if the ordering strategy is sorting on bucket count descending.
     *
     * @param order bucket ordering strategy to check.
     * @return {@code true} if the ordering strategy is sorting on bucket count descending, {@code false} otherwise.
     */
    public static boolean isCountDesc(BucketOrder order) {
        return isOrder(order, COUNT_DESC);
    }

    /**
     * Determine if the ordering strategy is sorting on bucket key (ascending or descending).
     *
     * @param order bucket ordering strategy to check.
     * @return {@code true} if the ordering strategy is sorting on bucket key, {@code false} otherwise.
     */
    public static boolean isKeyOrder(BucketOrder order) {
        return isOrder(order, KEY_ASC) || isOrder(order, KEY_DESC);
    }

    /**
     * Determine if the ordering strategy is sorting on bucket key ascending.
     *
     * @param order bucket ordering strategy to check.
     * @return {@code true} if the ordering strategy is sorting on bucket key ascending, {@code false} otherwise.
     */
    public static boolean isKeyAsc(BucketOrder order) {
        return isOrder(order, KEY_ASC);
    }

    /**
     * Determine if the ordering strategy is sorting on bucket key descending.
     *
     * @param order bucket ordering strategy to check.
     * @return {@code true} if the ordering strategy is sorting on bucket key descending, {@code false} otherwise.
     */
    public static boolean isKeyDesc(BucketOrder order) {
        return isOrder(order, KEY_DESC);
    }

    /**
     * Determine if the ordering strategy matches the expected one.
     *
     * @param order    bucket ordering strategy to check. If this is a {@link CompoundOrder} the first element will be
     *                 check instead.
     * @param expected expected  bucket ordering strategy.
     * @return {@code true} if the order matches, {@code false} otherwise.
     */
    private static boolean isOrder(BucketOrder order, BucketOrder expected) {
        if (order == expected) {
            return true;
        } else if (order instanceof CompoundOrder) {
            // check if its a compound order with the first element that matches
            List<BucketOrder> orders = ((CompoundOrder) order).orderElements;
            if (orders.size() >= 1) {
                return isOrder(orders.get(0), expected);
            }
        }
        return false;
    }

    /**
     * Contains logic for reading/writing {@link BucketOrder} from/to streams.
     */
    public static class Streams {

        /**
         * Read a {@link BucketOrder} from a {@link StreamInput}.
         *
         * @param in stream with order data to read.
         * @return order read from the stream
         * @throws IOException on error reading from the stream.
         */
        public static BucketOrder readOrder(StreamInput in) throws IOException {
            byte id = in.readByte();
            switch (id) {
                case COUNT_DESC_ID: return COUNT_DESC;
                case COUNT_ASC_ID: return COUNT_ASC;
                case KEY_DESC_ID: return KEY_DESC;
                case KEY_ASC_ID: return KEY_ASC;
                case Aggregation.ID:
                    boolean asc = in.readBoolean();
                    String key = in.readString();
                    return new Aggregation(key, asc);
                case CompoundOrder.ID:
                    int size = in.readVInt();
                    List<BucketOrder> compoundOrder = new ArrayList<>(size);
                    for (int i = 0; i < size; i++) {
                        compoundOrder.add(Streams.readOrder(in));
                    }
                    return new CompoundOrder(compoundOrder, false);
                default:
                    throw new RuntimeException("unknown order id [" + id + "]");
            }
        }

        /**
         * ONLY FOR HISTOGRAM ORDER: Backwards compatibility logic to read a {@link BucketOrder} from a {@link StreamInput}.
         *
         * @param in           stream with order data to read.
         * @param bwcOrderFlag {@code true} to check {@code in.readBoolean()} in the backwards compat logic before reading
         *                     the order. {@code false} to skip this flag (order always present).
         * @return order read from the stream
         * @throws IOException on error reading from the stream.
         */
        public static BucketOrder readHistogramOrder(StreamInput in, boolean bwcOrderFlag) throws IOException {
            if (in.getVersion().onOrAfter(Version.V_6_0_0_alpha2)) {
                return Streams.readOrder(in);
            } else { // backwards compat logic
                if (bwcOrderFlag == false || in.readBoolean()) {
                    // translate the old histogram order IDs to the new order objects
                    byte id = in.readByte();
                    switch (id) {
                        case 1: return KEY_ASC;
                        case 2: return KEY_DESC;
                        case 3: return COUNT_ASC;
                        case 4: return COUNT_DESC;
                        case 0: // aggregation order stream logic is backwards compatible
                            boolean asc = in.readBoolean();
                            String key = in.readString();
                            return new Aggregation(key, asc);
                        default: // not expecting compound order ID
                            throw new RuntimeException("unknown histogram order id [" + id + "]");
                    }
                } else { // default to _key asc if no order specified
                    return KEY_ASC;
                }
            }
        }

        /**
         * Write a {@link BucketOrder} to a {@link StreamOutput}.
         *
         * @param order order to write to the stream.
         * @param out   stream to write the order to.
         * @throws IOException on error writing to the stream.
         */
        public static void writeOrder(BucketOrder order, StreamOutput out) throws IOException {
            out.writeByte(order.id());
            if (order instanceof Aggregation) {
                Aggregation aggregationOrder = (Aggregation) order;
                out.writeBoolean(aggregationOrder.asc);
                out.writeString(aggregationOrder.path().toString());
            } else if (order instanceof CompoundOrder) {
                CompoundOrder compoundOrder = (CompoundOrder) order;
                out.writeVInt(compoundOrder.orderElements.size());
                for (BucketOrder innerOrder : compoundOrder.orderElements) {
                    innerOrder.writeTo(out);
                }
            }
        }

        /**
         * ONLY FOR HISTOGRAM ORDER: Backwards compatibility logic to write a {@link BucketOrder} to a stream.
         *
         * @param order        order to write to the stream.
         * @param out          stream to write the order to.
         * @param bwcOrderFlag {@code true} to always {@code out.writeBoolean(true)} for the backwards compat logic before
         *                     writing the order. {@code false} to skip this flag.
         * @throws IOException on error writing to the stream.
         */
        public static void writeHistogramOrder(BucketOrder order, StreamOutput out, boolean bwcOrderFlag) throws IOException {
            if (out.getVersion().onOrAfter(Version.V_6_0_0_alpha2)) {
                order.writeTo(out);
            } else { // backwards compat logic
                if(bwcOrderFlag) { // need to add flag that determines if order exists
                    out.writeBoolean(true); // order always exists
                }
                if (order instanceof CompoundOrder) {
                    // older versions do not support histogram compound order; the best we can do here is use the first order.
                    order = ((CompoundOrder) order).orderElements.get(0);
                }
                if (order instanceof Aggregation) {
                    // aggregation order stream logic is backwards compatible
                    order.writeTo(out);
                } else {
                    // convert the new order IDs to the old histogram order IDs.
                    byte id;
                    switch (order.id()) {
                        case COUNT_DESC_ID: id = 4; break;
                        case COUNT_ASC_ID: id = 3; break;
                        case KEY_DESC_ID: id = 2; break;
                        case KEY_ASC_ID: id = 1; break;
                        default: throw new RuntimeException("unknown order id [" + order.id() + "]");
                    }
                    out.writeByte(id);
                }
            }
        }
    }

    /**
     * Contains logic for parsing a {@link BucketOrder} from a {@link XContentParser}.
     */
    public static class Parser {

        private static final DeprecationLogger DEPRECATION_LOGGER =
            new DeprecationLogger(Loggers.getLogger(Parser.class));

        /**
         * Parse a {@link BucketOrder} from {@link XContent}.
         *
         * @param parser  for parsing {@link XContent} that contains the order.
         * @return bucket ordering strategy
         * @throws IOException on error a {@link XContent} parsing error.
         */
        public static BucketOrder parseOrderParam(XContentParser parser) throws IOException {
            XContentParser.Token token;
            String orderKey = null;
            boolean orderAsc = false;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    orderKey = parser.currentName();
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    String dir = parser.text();
                    if ("asc".equalsIgnoreCase(dir)) {
                        orderAsc = true;
                    } else if ("desc".equalsIgnoreCase(dir)) {
                        orderAsc = false;
                    } else {
                        throw new ParsingException(parser.getTokenLocation(),
                            "Unknown order direction [" + dir + "]");
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                        "Unexpected token [" + token + "] for [order]");
                }
            }
            if (orderKey == null) {
                throw new ParsingException(parser.getTokenLocation(),
                    "Must specify at least one field for [order]");
            }
            // _term and _time order deprecated in 6.0; replaced by _key
            if ("_term".equals(orderKey) || "_time".equals(orderKey)) {
                DEPRECATION_LOGGER.deprecated("Deprecated aggregation order key [{}] used, replaced by [_key]", orderKey);
            }
            switch (orderKey) {
                case "_term":
                case "_time":
                case "_key":
                    return orderAsc ? KEY_ASC : KEY_DESC;
                case "_count":
                    return orderAsc ? COUNT_ASC : COUNT_DESC;
                default: // assume all other orders are sorting on a sub-aggregation. Validation occurs later.
                    return aggregation(orderKey, orderAsc);
            }
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, key, asc);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        InternalOrder other = (InternalOrder) obj;
        return Objects.equals(id, other.id)
            && Objects.equals(key, other.key)
            && Objects.equals(asc, other.asc);
    }
}
