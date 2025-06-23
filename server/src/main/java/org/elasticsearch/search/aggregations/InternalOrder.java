/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.DelayableWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.Aggregator.BucketComparator;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.BucketAndOrd;
import org.elasticsearch.search.aggregations.support.AggregationPath;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.sort.SortValue;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;

/**
 * Implementations for {@link Bucket} ordering strategies.
 */
public abstract class InternalOrder extends BucketOrder {
    // TODO merge the contents of this file into BucketOrder. The way it is now is relic.

    /**
     * {@link Bucket} ordering strategy to sort by a sub-aggregation.
     */
    public static class Aggregation extends InternalOrder {
        static final byte ID = 0;
        private final SortOrder order;
        private final AggregationPath path;

        /**
         * Create a new ordering strategy to sort by a sub-aggregation.
         *
         * @param path path to the sub-aggregation to sort on.
         * @param asc  direction to sort by: {@code true} for ascending, {@code false} for descending.
         * @see AggregationPath
         */
        Aggregation(String path, boolean asc) {
            order = asc ? SortOrder.ASC : SortOrder.DESC;
            this.path = AggregationPath.parse(path);
        }

        public AggregationPath path() {
            return path;
        }

        @Override
        public <T extends Bucket> Comparator<BucketAndOrd<T>> partiallyBuiltBucketComparator(Aggregator aggregator) {
            try {
                BucketComparator bucketComparator = path.bucketComparator(aggregator, order);
                return (lhs, rhs) -> bucketComparator.compare(lhs.ord, rhs.ord);
            } catch (IllegalArgumentException e) {
                throw new AggregationExecutionException.InvalidPath("Invalid aggregation order path [" + path + "]. " + e.getMessage(), e);
            }
        }

        @Override
        public Comparator<Bucket> comparator() {
            return (lhs, rhs) -> {
                final SortValue l = path.resolveValue(lhs.getAggregations());
                final SortValue r = path.resolveValue(rhs.getAggregations());
                int compareResult = l.compareTo(r);
                return order == SortOrder.ASC ? compareResult : -compareResult;
            };
        }

        @Override
        <B extends InternalMultiBucketAggregation.InternalBucket> Comparator<DelayedBucket<B>> delayedBucketComparator(
            BiFunction<List<B>, AggregationReduceContext, B> reduce,
            AggregationReduceContext reduceContext
        ) {
            Comparator<Bucket> comparator = comparator();
            /*
             * Reduce the buckets if we haven't already so we can get at the
             * sub-aggregations. With enough code we could avoid this but
             * we haven't written that code....
             */
            return (lhs, rhs) -> comparator.compare(lhs.reduced(reduce, reduceContext), rhs.reduced(reduce, reduceContext));
        }

        @Override
        byte id() {
            return ID;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().field(path.toString(), order.toString()).endObject();
        }

        @Override
        public int hashCode() {
            return Objects.hash(path, order);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Aggregation other = (Aggregation) obj;
            return Objects.equals(path, other.path) && Objects.equals(order, other.order);
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
            if (this.orderElements.isEmpty()) {
                throw new IllegalArgumentException("empty compound order not supported");
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
        public <T extends Bucket> Comparator<BucketAndOrd<T>> partiallyBuiltBucketComparator(Aggregator aggregator) {
            Iterator<BucketOrder> iterator = orderElements.iterator();
            Comparator<BucketAndOrd<T>> comparator = iterator.next().partiallyBuiltBucketComparator(aggregator);
            while (iterator.hasNext()) {
                comparator = comparator.thenComparing(iterator.next().partiallyBuiltBucketComparator(aggregator));
            }
            return comparator;
        }

        @Override
        public Comparator<Bucket> comparator() {
            Iterator<BucketOrder> iterator = orderElements.iterator();
            Comparator<Bucket> comparator = iterator.next().comparator();
            while (iterator.hasNext()) {
                comparator = comparator.thenComparing(iterator.next().comparator());
            }
            return comparator;
        }

        @Override
        <B extends InternalMultiBucketAggregation.InternalBucket> Comparator<DelayedBucket<B>> delayedBucketComparator(
            BiFunction<List<B>, AggregationReduceContext, B> reduce,
            AggregationReduceContext reduceContext
        ) {
            Iterator<BucketOrder> iterator = orderElements.iterator();
            Comparator<DelayedBucket<B>> comparator = iterator.next().delayedBucketComparator(reduce, reduceContext);
            while (iterator.hasNext()) {
                comparator = comparator.thenComparing(iterator.next().delayedBucketComparator(reduce, reduceContext));
            }
            return comparator;
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
    }

    /**
     * {@link BucketOrder} implementation for simple, fixed orders like
     * {@link InternalOrder#COUNT_ASC}. Complex implementations should not
     * use this.
     */
    private static class SimpleOrder extends InternalOrder {
        private final byte id;
        private final String key;
        private final SortOrder order;
        private final Comparator<Bucket> comparator;
        private final Comparator<DelayedBucket<? extends Bucket>> delayedBucketCompator;

        SimpleOrder(
            byte id,
            String key,
            SortOrder order,
            Comparator<Bucket> comparator,
            Comparator<DelayedBucket<? extends Bucket>> delayedBucketCompator
        ) {
            this.id = id;
            this.key = key;
            this.order = order;
            this.comparator = comparator;
            this.delayedBucketCompator = delayedBucketCompator;
        }

        @Override
        public Comparator<Bucket> comparator() {
            return comparator;
        }

        @SuppressWarnings({ "rawtypes", "unchecked" })
        @Override
        <B extends InternalMultiBucketAggregation.InternalBucket> Comparator<DelayedBucket<B>> delayedBucketComparator(
            BiFunction<List<B>, AggregationReduceContext, B> reduce,
            AggregationReduceContext reduceContext
        ) {
            return (Comparator) delayedBucketCompator;
        }

        @Override
        byte id() {
            return id;
        }

        @Override
        public <T extends Bucket> Comparator<BucketAndOrd<T>> partiallyBuiltBucketComparator(Aggregator aggregator) {
            Comparator<Bucket> comparator = comparator();
            return (lhs, rhs) -> comparator.compare(lhs.bucket, rhs.bucket);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().field(key, order.toString()).endObject();
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, key, order);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            SimpleOrder other = (SimpleOrder) obj;
            return Objects.equals(id, other.id) && Objects.equals(key, other.key) && Objects.equals(order, other.order);
        }
    }

    private static final byte COUNT_DESC_ID = 1;
    private static final byte COUNT_ASC_ID = 2;
    private static final byte KEY_DESC_ID = 3;
    private static final byte KEY_ASC_ID = 4;

    /**
     * Order by the (higher) count of each bucket.
     */
    static final InternalOrder COUNT_DESC = new SimpleOrder(
        COUNT_DESC_ID,
        "_count",
        SortOrder.DESC,
        comparingCounts().reversed(),
        comparingDelayedCounts().reversed()
    );

    /**
     * Order by the (lower) count of each bucket.
     */
    static final InternalOrder COUNT_ASC = new SimpleOrder(
        COUNT_ASC_ID,
        "_count",
        SortOrder.ASC,
        comparingCounts(),
        comparingDelayedCounts()
    );

    /**
     * Order by the key of each bucket descending.
     */
    static final InternalOrder KEY_DESC = new SimpleOrder(
        KEY_DESC_ID,
        "_key",
        SortOrder.DESC,
        comparingKeys().reversed(),
        comparingDelayedKeys().reversed()
    );

    /**
     * Order by the key of each bucket ascending.
     */
    static final InternalOrder KEY_ASC = new SimpleOrder(KEY_ASC_ID, "_key", SortOrder.ASC, comparingKeys(), comparingDelayedKeys());

    /**
     * @return compare by {@link Bucket#getDocCount()}.
     */
    private static Comparator<Bucket> comparingCounts() {
        return Comparator.comparingLong(Bucket::getDocCount);
    }

    /**
     * @return compare by {@link Bucket#getDocCount()} that will be in the bucket once it is reduced
     */
    private static Comparator<DelayedBucket<? extends Bucket>> comparingDelayedCounts() {
        return Comparator.comparingLong(DelayedBucket::getDocCount);
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
     * @return compare by {@link Bucket#getKey()} that will be in the bucket once it is reduced
     */
    private static Comparator<DelayedBucket<? extends Bucket>> comparingDelayedKeys() {
        return DelayedBucket::compareKey;
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
        return order == expected || (order instanceof CompoundOrder compoundOrder && compoundOrder.orderElements.getFirst() == expected);
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
            return readOrder(in, true);
        }

        private static BucketOrder readOrder(StreamInput in, boolean dedupe) throws IOException {
            byte id = in.readByte();
            switch (id) {
                case COUNT_DESC_ID:
                    return COUNT_DESC;
                case COUNT_ASC_ID:
                    return COUNT_ASC;
                case KEY_DESC_ID:
                    return KEY_DESC;
                case KEY_ASC_ID:
                    return KEY_ASC;
                case Aggregation.ID:
                    boolean asc = in.readBoolean();
                    String key = in.readString();
                    if (dedupe && in instanceof DelayableWriteable.Deduplicator bo) {
                        return bo.deduplicate(new Aggregation(key, asc));
                    }
                    return new Aggregation(key, asc);
                case CompoundOrder.ID:
                    int size = in.readVInt();
                    List<BucketOrder> compoundOrder = new ArrayList<>(size);
                    for (int i = 0; i < size; i++) {
                        compoundOrder.add(Streams.readOrder(in, false));
                    }
                    if (dedupe && in instanceof DelayableWriteable.Deduplicator bo) {
                        return bo.deduplicate(new CompoundOrder(compoundOrder, false));
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
         * @return order read from the stream
         * @throws IOException on error reading from the stream.
         */
        public static BucketOrder readHistogramOrder(StreamInput in) throws IOException {
            return Streams.readOrder(in);
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
            if (order instanceof Aggregation aggregationOrder) {
                out.writeBoolean(aggregationOrder.order == SortOrder.ASC);
                out.writeString(aggregationOrder.path().toString());
            } else if (order instanceof CompoundOrder compoundOrder) {
                out.writeCollection(compoundOrder.orderElements);
            }
        }

        /**
         * ONLY FOR HISTOGRAM ORDER: Backwards compatibility logic to write a {@link BucketOrder} to a stream.
         *
         * @param order        order to write to the stream.
         * @param out          stream to write the order to.
         * @throws IOException on error writing to the stream.
         */
        public static void writeHistogramOrder(BucketOrder order, StreamOutput out) throws IOException {
            order.writeTo(out);
        }
    }

    /**
     * Contains logic for parsing a {@link BucketOrder} from a {@link XContentParser}.
     */
    public static class Parser {

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
                        throw new ParsingException(parser.getTokenLocation(), "Unknown order direction [" + dir + "]");
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "Unexpected token [" + token + "] for [order]");
                }
            }
            if (orderKey == null) {
                throw new ParsingException(parser.getTokenLocation(), "Must specify at least one field for [order]");
            }
            return switch (orderKey) {
                case "_key" -> orderAsc ? KEY_ASC : KEY_DESC;
                case "_count" -> orderAsc ? COUNT_ASC : COUNT_DESC;
                default -> // assume all other orders are sorting on a sub-aggregation. Validation occurs later.
                    aggregation(orderKey, orderAsc);
            };
        }
    }
}
