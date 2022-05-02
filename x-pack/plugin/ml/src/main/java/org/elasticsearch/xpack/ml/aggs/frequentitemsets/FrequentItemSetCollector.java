/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.frequentitemsets;

import org.apache.lucene.util.LongsRef;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.aggregations.Aggregation.CommonFields;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Collector for frequent item sets.
 *
 * The collector keeps the best top-n sets found during mining and reports the minimum doc count required to enter.
 * With other words the last of top-n. This is useful to skip over candidates quickly.
 *
 * Technically this is implemented as priority queue which is implemented as heap with the last item on top.
 *
 * To get to the top-n results pop the collector until it is empty and than reverse the order.
 */
public class FrequentItemSetCollector {

    /**
     * Container for a single frequent itemset
     */
    public class FrequentItemSet implements ToXContent {

        private LongsRef items;
        private long docCount;

        // every set has a unique id, required for the outer logic
        private int id;

        private FrequentItemSet() {
            this.id = -1;
            this.items = new LongsRef(10);
            this.docCount = -1;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            final long totalTransactionCount = transactionStore.getTotalTransactionCount();

            builder.startObject();
            builder.startObject(CommonFields.KEY.getPreferredName());
            Map<String, List<String>> frequentItemsKeyValues = new HashMap<>();

            for (int i = 0; i < items.length; ++i) {
                Tuple<String, String> item = transactionStore.getItem(items.longs[i]);
                if (frequentItemsKeyValues.containsKey(item.v1())) {
                    frequentItemsKeyValues.get(item.v1()).add(item.v2());
                } else {
                    List<String> l = new ArrayList<>();
                    l.add(item.v2());
                    frequentItemsKeyValues.put(item.v1(), l);
                }
            }

            for (Entry<String, List<String>> item : frequentItemsKeyValues.entrySet()) {
                builder.field(item.getKey(), item.getValue());
            }
            builder.endObject();

            builder.field(CommonFields.DOC_COUNT.getPreferredName(), docCount);
            builder.field("support", (double) docCount / totalTransactionCount);
            builder.endObject();
            return builder;
        }

        public long getDocCount() {
            return docCount;
        }

        public LongsRef getItems() {
            return items;
        }

        public int getId() {
            return id;
        }

        public int size() {
            return items.length;
        }

        private void reset(int id, List<Long> items, long docCount) {
            if (items.size() > this.items.length) {
                this.items = new LongsRef(items.size());
            }

            for (int i = 0; i < items.size(); ++i) {
                this.items.longs[i] = items.get(i);
            }

            this.items.length = items.size();
            this.docCount = docCount;
            this.id = id;
        }
    }

    private static class FrequentItemSetPriorityQueue extends PriorityQueue<FrequentItemSet> {
        FrequentItemSetPriorityQueue(int size) {
            super(size);
        }

        @Override
        protected boolean lessThan(FrequentItemSet a, FrequentItemSet b) {
            if (a.docCount == b.docCount) {
                if (a.size() == b.size()) {
                    return Arrays.compare(a.items.longs, 0, a.items.length, b.items.longs, 0, b.items.length) < 0;
                }
                return a.size() < b.size();
            }

            return a.docCount < b.docCount;
        }
    }

    private final TransactionStore transactionStore;
    private final FrequentItemSetPriorityQueue queue;

    // index for closed item set de-duplication
    private final Map<Long, List<FrequentItemSet>> frequentItemsByCount;

    private final int size;
    private final long min;

    private int count = 0;
    private FrequentItemSet spareSet = new FrequentItemSet();

    public FrequentItemSetCollector(TransactionStore transactionStore, int size, long min) {
        this.transactionStore = transactionStore;
        this.size = size;
        this.min = min;
        queue = new FrequentItemSetPriorityQueue(size);
        frequentItemsByCount = Maps.newMapWithExpectedSize(size / 10);
    }

    public FrequentItemSet pop() {
        return queue.pop();
    }

    public int size() {
        return queue.size();
    }

    /**
     * Add an itemSet to the collector, the set is only accepted if the given doc count is larger than the
     * minimum count and other criteria like the closed set criteria are met.
     *
     * Note: If added to the collector the given item set is not reused but deep copied
     *
     * @param itemSet the itemSet
     * @param docCount the doc count for this set
     * @return the new minimum doc count necessary to enter the collector
     */
    public long add(List<Long> itemSet, long docCount) {
        if (queue.top() == null || queue.size() < size || docCount > queue.top().getDocCount()) {

            // closed set criteria: don't add if we already store a superset
            if (hasSuperSet(itemSet, docCount) == false) {
                spareSet.reset(count++, itemSet, docCount);
                FrequentItemSet newItemSet = spareSet;
                FrequentItemSet removedItemSet = queue.insertWithOverflow(spareSet);
                if (removedItemSet != null) {
                    // remove item from frequentItemsByCount
                    frequentItemsByCount.compute(removedItemSet.getDocCount(), (k, sets) -> {

                        // short cut, if there is only 1, it must be the one we are looking for
                        if (sets.size() == 1) {
                            return null;
                        }

                        sets.remove(removedItemSet);
                        return sets;
                    });
                    spareSet = removedItemSet;
                } else {
                    spareSet = new FrequentItemSet();
                }

                frequentItemsByCount.compute(newItemSet.getDocCount(), (k, sets) -> {
                    if (sets == null) {
                        sets = new ArrayList<>();
                    }
                    sets.add(newItemSet);
                    return sets;
                });
            }
        }

        // return the minimum doc count this collector takes
        return queue.size() < size ? min : queue.top().getDocCount();
    }

    // for unit tests
    Map<Long, List<FrequentItemSet>> getFrequentItemsByCount() {
        return frequentItemsByCount;
    }

    /**
     * Criteria for closed sets
     *
     * A item set is called closed if no superset has the same support.
     *
     * E.g.
     *
     * [cat, dog, crocodile] -> 0.2
     * [cat, dog, crocodile, river] -> 0.2
     *
     * [cat, dog, crocodile] gets skipped, because [cat, dog, crocodile, river] has the same support.
     *
     */
    private boolean hasSuperSet(List<Long> itemSet, long docCount) {
        List<FrequentItemSet> setsThatShareSameDocCount = frequentItemsByCount.get(docCount);
        boolean foundSuperSet = false;
        if (setsThatShareSameDocCount != null) {
            for (FrequentItemSet otherSet : setsThatShareSameDocCount) {
                if (otherSet.size() < itemSet.size()) {
                    continue;
                }

                int pos = 0;
                LongsRef otherItemSet = otherSet.getItems();
                for (int i = 0; i < otherItemSet.length; ++i) {
                    if (otherItemSet.longs[i] == itemSet.get(pos)) {
                        pos++;
                        if (pos == itemSet.size()) {
                            foundSuperSet = true;
                            break;
                        }
                    }
                }

                if (foundSuperSet) {
                    break;
                }
            }
        }
        return foundSuperSet;
    }

}
