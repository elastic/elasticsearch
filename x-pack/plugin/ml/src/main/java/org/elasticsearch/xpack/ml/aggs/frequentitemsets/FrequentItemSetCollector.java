/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.frequentitemsets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.aggregations.Aggregation.CommonFields;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.ml.aggs.frequentitemsets.TransactionStore.TopItemIds;
import org.elasticsearch.xpack.ml.aggs.frequentitemsets.mr.ItemSetMapReduceValueSource.Field;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

/**
 * Collector for frequent item sets.
 *
 * The collector keeps the best top-n sets found during mining and reports the minimum doc count required to enter.
 * With other words the last of top-n. This is useful to skip over candidates quickly.
 *
 * Technically this is implemented as priority queue which is implemented as heap with the last item on top.
 *
 * To get to the top-n results pop the collector until it is empty and then reverse the order.
 */
public final class FrequentItemSetCollector {

    private static final Logger logger = LogManager.getLogger(FrequentItemSetCollector.class);

    public static class FrequentItemSet implements ToXContent, Writeable {
        private final Map<String, List<Object>> fields;
        private final double support;

        // mutable for sampling
        private long docCount;

        public FrequentItemSet(Map<String, List<Object>> fields, long docCount, double support) {
            this.fields = Collections.unmodifiableMap(fields);
            this.docCount = docCount;
            this.support = support;
        }

        public FrequentItemSet(StreamInput in) throws IOException {
            this.fields = in.readMapOfLists(StreamInput::readGenericValue);
            this.docCount = in.readVLong();
            this.support = in.readDouble();
        }

        public long getDocCount() {
            return docCount;
        }

        public double getSupport() {
            return support;
        }

        public Map<String, List<Object>> getFields() {
            return fields;
        }

        public void setDocCount(long docCount) {
            this.docCount = docCount;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startObject(CommonFields.KEY.getPreferredName());
            for (Entry<String, List<Object>> item : fields.entrySet()) {
                builder.field(item.getKey(), item.getValue());
            }
            builder.endObject();

            builder.field(CommonFields.DOC_COUNT.getPreferredName(), getDocCount());
            builder.field("support", support);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMapOfLists(fields, StreamOutput::writeString, StreamOutput::writeGenericValue);
            out.writeVLong(getDocCount());
            out.writeDouble(support);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }

            if (other == null || getClass() != other.getClass()) {
                return false;
            }

            final FrequentItemSet that = (FrequentItemSet) other;

            return this.docCount == that.docCount
                && this.support == that.support
                && this.fields.size() == that.fields.size()
                && this.fields.entrySet().stream().allMatch(e -> e.getValue().equals(that.fields.get(e.getKey())));
        }

        @Override
        public int hashCode() {
            return Objects.hash(docCount, support, fields);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("{\"key\": {");

            boolean addComma = false;
            for (Entry<String, List<Object>> item : fields.entrySet()) {
                if (addComma) {
                    sb.append(", ");
                }
                sb.append("\"" + item.getKey() + "\": {");
                sb.append(Strings.collectionToDelimitedString(item.getValue(), ", "));
                sb.append("}");
                addComma = true;
            }
            sb.append("}, \"doc_count\": ");
            sb.append(docCount);
            sb.append(", \"support\": ");
            sb.append(support);
            sb.append("}");

            return sb.toString();
        }

    }

    /**
     * Container for a single frequent itemset
     *
     * package private for unit tests
     */
    class FrequentItemSetCandidate {

        private static final int STARTBITS = 64;
        private ItemSetBitSet items;
        private long docCount;

        // every set has a unique id, required for the outer logic
        private int id;

        private FrequentItemSetCandidate() {
            this.id = -1;
            this.items = new ItemSetBitSet(STARTBITS);
            this.docCount = -1;
        }

        FrequentItemSet toFrequentItemSet(List<Field> fields) throws IOException {
            Map<String, List<Object>> frequentItemsKeyValues = new HashMap<>();

            int pos = items.nextSetBit(0);
            while (pos > 0) {
                Tuple<Integer, Object> item = transactionStore.getItem(topItemIds.getItemIdAt(pos - 1));
                assert item.v1() < fields.size() : "item id exceed number of given items, did you configure eclat correctly?";
                final Field field = fields.get(item.v1());
                Object formattedValue = field.formatValue(item.v2());
                String fieldName = fields.get(item.v1()).getName();

                if (frequentItemsKeyValues.containsKey(fieldName)) {
                    frequentItemsKeyValues.get(fieldName).add(formattedValue);
                } else {
                    List<Object> l = new ArrayList<>();
                    l.add(formattedValue);
                    frequentItemsKeyValues.put(fieldName, l);
                }

                pos = items.nextSetBit(++pos);
            }

            return new FrequentItemSet(frequentItemsKeyValues, docCount, (double) docCount / transactionStore.getTotalTransactionCount());
        }

        long getDocCount() {
            return docCount;
        }

        ItemSetBitSet getItems() {
            return items;
        }

        int getId() {
            return id;
        }

        int size() {
            return items.cardinality();
        }

        private void reset(int id, ItemSetBitSet items, long docCount) {
            this.items.reset(items);
            this.docCount = docCount;
            this.id = id;
        }
    }

    static class FrequentItemSetPriorityQueue extends PriorityQueue<FrequentItemSetCandidate> {
        FrequentItemSetPriorityQueue(int size) {
            super(size);
        }

        @Override
        protected boolean lessThan(FrequentItemSetCandidate a, FrequentItemSetCandidate b) {
            if (a.docCount == b.docCount) {
                return ItemSetBitSet.compare(a.items, b.items) < 0;
            }

            return a.docCount < b.docCount;
        }
    }

    private final TransactionStore transactionStore;
    private final TopItemIds topItemIds;
    private final FrequentItemSetPriorityQueue queue;

    // index for closed item set de-duplication
    private final Map<Long, List<FrequentItemSetCandidate>> frequentItemsByCount;

    private final int size;
    private final long min;

    private int count = 0;
    private FrequentItemSetCandidate spareSet = new FrequentItemSetCandidate();

    public FrequentItemSetCollector(TransactionStore transactionStore, TopItemIds topItemIds, int size, long min) {
        this.transactionStore = transactionStore;
        this.topItemIds = topItemIds;
        this.size = size;
        this.min = min;
        queue = new FrequentItemSetPriorityQueue(size);
        frequentItemsByCount = Maps.newMapWithExpectedSize(size / 10);
    }

    public FrequentItemSet[] finalizeAndGetResults(List<Field> fields) throws IOException {
        FrequentItemSet[] topFrequentItems = new FrequentItemSet[size()];
        for (int i = topFrequentItems.length - 1; i >= 0; i--) {
            topFrequentItems[i] = queue.pop().toFrequentItemSet(fields);
        }
        return topFrequentItems;
    }

    public int size() {
        return queue.size();
    }

    /**
     * Add an itemSet to the collector, the set is only accepted if the given doc count is larger than the
     * minimum count and other criteria like the closed set criteria are met.
     *
     * Note: If added to the collector the given item set is not reused but deep copied
     * @param itemSet the itemSet
     * @param docCount the doc count for this set
     *
     * @return the new minimum doc count necessary to enter the collector
     */
    public long add(ItemSetBitSet itemSet, long docCount) {
        logger.trace("add itemset with {} items, doc_count {}", itemSet.cardinality(), docCount);

        // if the queue is full, shortcut if itemset has a lower count or fewer items than the last set in the queue
        if (queue.top() != null
            && queue.size() == size
            && (docCount < queue.top().getDocCount()
                || (docCount == queue.top().getDocCount() && itemSet.cardinality() <= queue.top().size()))) {
            return queue.top().getDocCount();
        }

        /**
         * Criteria for closed sets
         *
         * An item set is called closed if no superset has the same support.
         * e.g.
         *
         * [cat, dog, crocodile] -> 0.2
         * [cat, dog, crocodile, river] -> 0.2
         *
         * [cat, dog, crocodile] gets skipped, because [cat, dog, crocodile, river] has the same support.
         * if [cat, dog, crocodile] was added first, [cat, dog, crocodile, river] replaces [cat, dog, crocodile]
         */
        List<FrequentItemSetCandidate> setsThatShareSameDocCountBits = frequentItemsByCount.get(docCount);

        if (setsThatShareSameDocCountBits != null) {
            for (FrequentItemSetCandidate otherSet : setsThatShareSameDocCountBits) {
                ItemSetBitSet.SetRelation relation = itemSet.setRelation(otherSet.getItems());

                if (relation.equals(ItemSetBitSet.SetRelation.SUPER_SET)) {
                    removeFromFrequentItemsByCount(otherSet);
                    queue.remove(otherSet);
                    break;
                }

                if (relation.equals(ItemSetBitSet.SetRelation.SUB_SET)) {
                    // closed set criteria: don't add if we already store a superset
                    return queue.size() < size ? min : queue.top().getDocCount();
                }
            }
        }

        spareSet.reset(count++, itemSet, docCount);
        FrequentItemSetCandidate newItemSet = spareSet;
        FrequentItemSetCandidate removedItemSet = queue.insertWithOverflow(spareSet);
        if (removedItemSet != null) {
            removeFromFrequentItemsByCount(removedItemSet);
            spareSet = removedItemSet;
        } else {
            spareSet = new FrequentItemSetCandidate();
        }

        frequentItemsByCount.computeIfAbsent(newItemSet.getDocCount(), (k) -> new ArrayList<>()).add(newItemSet);
        // return the minimum doc count this collector takes
        return queue.size() < size ? min : queue.top().getDocCount();
    }

    private void removeFromFrequentItemsByCount(FrequentItemSetCandidate removedItemSet) {
        // remove item from frequentItemsByCount
        frequentItemsByCount.compute(removedItemSet.getDocCount(), (k, sets) -> {

            // short cut, if there is only 1, it must be the one we are looking for
            if (sets.size() == 1) {
                return null;
            }

            sets.remove(removedItemSet);
            return sets;
        });
    }

    // for unit tests
    Map<Long, List<FrequentItemSetCandidate>> getFrequentItemsByCount() {
        return frequentItemsByCount;
    }

    FrequentItemSetPriorityQueue getQueue() {
        return queue;
    }

    FrequentItemSetCandidate getLastSet() {
        return queue.top();
    }
}
