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

public class FrequentItemSetCollector {

    public class FrequentItemSet implements ToXContent {

        private final long[] items;
        private final long docCount;

        // every set has a unique id, required for the outer logic
        private final int id;

        FrequentItemSet(int id, long[] items, long docCount) {
            this.items = items;
            this.docCount = docCount;
            this.id = id;

            /*logger.info(
                "add fi set {} ({}) with dc: {}",
                Strings.collectionToDelimitedString(items.stream().map(id -> {
                    try {
                        return transactionStore.getItem(id);
                    } catch (IOException e) {
                        return "IO ERROR";
                    }
                }).collect(Collectors.toList()), ","),
                Strings.collectionToDelimitedString(items, ","),
                docCount

            );*/
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            final long totalTransactionCount = transactionStore.getTotalTransactionCount();

            builder.startObject();
            builder.startObject(CommonFields.KEY.getPreferredName());
            Map<String, List<String>> frequentItemsKeyValues = new HashMap<>();
            for (Long id : items) {
                Tuple<String, String> item = transactionStore.getItem(id);
                if (frequentItemsKeyValues.containsKey(item.v1())) {
                    frequentItemsKeyValues.get(item.v1()).add(item.v2());
                } else {
                    List<String> l = new ArrayList<>();
                    l.add(item.v2());
                    frequentItemsKeyValues.put(item.v1(), l);
                }
            }

            for (Entry<String, List<String>> item : frequentItemsKeyValues.entrySet()) {
                // TODO: flatten single lists?
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

        public long[] getItems() {
            return items;
        }

        public int getId() {
            return id;
        }

        public int size() {
            return items.length;
        }
    }

    static class FrequentItemSetPriorityQueue extends PriorityQueue<FrequentItemSet> {
        FrequentItemSetPriorityQueue(int size) {
            super(size);
        }

        @Override
        protected boolean lessThan(FrequentItemSet a, FrequentItemSet b) {
            if (a.docCount == b.docCount) {
                if (a.size() == b.size()) {
                    return Arrays.compare(a.items, b.items) < 0;

                }
                return a.size() < b.size();
            }

            return a.docCount < b.docCount;
        }
    }

    private static final Logger logger = LogManager.getLogger(FrequentItemSetCollector.class);

    private final TransactionStore transactionStore;
    private final FrequentItemSetPriorityQueue queue;
    private final int size;
    private final Map<Long, List<FrequentItemSet>> frequentItemsByCount;
    private int count = 0;
    private final long min;

    public FrequentItemSetCollector(TransactionStore transactionStore, int size, long min) {
        this.transactionStore = transactionStore;
        this.size = size;
        this.min = min;
        queue = new FrequentItemSetPriorityQueue(size);
        frequentItemsByCount = new HashMap<>(size / 10);
    }

    public FrequentItemSet pop() {
        return queue.pop();
    }

    public int size() {
        return queue.size();
    }

    public long add(List<Long> itemSet, long docCount) {
        if (queue.top() == null || queue.size() < size || docCount > queue.top().getDocCount()) {

            // closed set criteria: don't add if we already store a superset
            List<FrequentItemSet> setsThatShareSameDocCount = getFrequentItemsByCount().get(docCount);
            boolean foundSuperSet = false;
            if (setsThatShareSameDocCount != null) {
                for (FrequentItemSet otherSet : setsThatShareSameDocCount) {
                    if (otherSet.size() < itemSet.size()) {
                        continue;
                    }

                    int pos = 0;
                    for (Long item : otherSet.getItems()) {
                        if (item == itemSet.get(pos)) {
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

            if (foundSuperSet == false) {
                // TODO: recycle instances
                // note: we have to copy the itemSet
                FrequentItemSet newItemSet = new FrequentItemSet(count++, itemSet.stream().mapToLong(l -> l).toArray(), docCount);
                FrequentItemSet removedItemSet = queue.insertWithOverflow(newItemSet);

                if (removedItemSet != null) {
                    // remove item from frequentItemsByCount
                    getFrequentItemsByCount().compute(removedItemSet.getDocCount(), (k, sets) -> {

                        // short cut, if there is only 1, it must be the one we are looking for
                        if (sets.size() == 1) {
                            return null;
                        }

                        sets.remove(removedItemSet);
                        return sets;
                    });
                }

                getFrequentItemsByCount().compute(newItemSet.getDocCount(), (k, sets) -> {
                    if (sets == null) {
                        sets = new ArrayList<>();
                    }
                    sets.add(newItemSet);
                    return sets;
                });
            }
        }

        // return the minimum doc count this collector takes
        // TODO: return doc count +1? what about the longer sets over smaller sets criteria?
        return queue.size() < size ? min : queue.top().getDocCount();
    }

    // testing only
    public Map<Long, List<FrequentItemSet>> getFrequentItemsByCount() {
        return frequentItemsByCount;
    }

}
