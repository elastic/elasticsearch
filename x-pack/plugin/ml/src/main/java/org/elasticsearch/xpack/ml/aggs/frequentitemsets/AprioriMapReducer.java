/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.frequentitemsets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.aggregations.Aggregation.CommonFields;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.ml.aggs.frequentitemsets.TransactionStore.TopItemIds;
import org.elasticsearch.xpack.ml.aggs.frequentitemsets.TransactionStore.TopTransactionIds;
import org.elasticsearch.xpack.ml.aggs.mapreduce.MapReducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class AprioriMapReducer implements MapReducer {
    private static final Logger logger = LogManager.getLogger(AprioriMapReducer.class);

    private static final int VERSION = 1;
    public static final String NAME = "frequent_items-apriori-" + VERSION;

    // TODO: do we need this parameter???
    private static final long maxSetSize = 10;

    private final String aggregationWritableName;

    private final double minimumSupport;
    private final int minimumSetSize;
    private final int size;

    private List<FrequentItemSet> frequentSets = null;

    private TransactionStore transactionStore;

    public AprioriMapReducer(String aggregationWritableName, double minimumSupport, int minimumSetSize, int size) {
        this.aggregationWritableName = aggregationWritableName;
        this.minimumSupport = minimumSupport;
        this.minimumSetSize = minimumSetSize;
        this.size = size;
        this.transactionStore = new TransactionStore(BigArrays.NON_RECYCLING_INSTANCE);
    }

    public AprioriMapReducer(StreamInput in) throws IOException {
        this.aggregationWritableName = in.readString();

        // parameters
        this.minimumSupport = in.readDouble();
        this.minimumSetSize = in.readInt();
        this.size = in.readInt();

        // data
        this.transactionStore = new TransactionStore(in, BigArrays.NON_RECYCLING_INSTANCE);

        // not send over the wire
        this.frequentSets = null;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public String getAggregationWritableName() {
        return aggregationWritableName;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(aggregationWritableName);

        // parameters
        out.writeDouble(minimumSupport);
        out.writeInt(minimumSetSize);
        out.writeInt(size);

        // data
        transactionStore.writeTo(out);
    }

    @Override
    public void map(Stream<Tuple<String, List<Object>>> keyValues) {
        transactionStore.add(keyValues);
    }

    @Override
    public void reduce(Stream<MapReducer> partitions) {
        TransactionStore transactionsReduced = new TransactionStore(BigArrays.NON_RECYCLING_INSTANCE);
        partitions.forEach(p -> {
            AprioriMapReducer aprioriPartition = (AprioriMapReducer) p;
            transactionsReduced.mergeAndClose(aprioriPartition.transactionStore);
        });

        transactionStore.close();
        transactionStore = transactionsReduced;
    }

    @Override
    public void reduceFinalize() throws IOException {
        transactionStore.prune(minimumSupport);
        apriori();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (frequentSets != null) {
            builder.field(CommonFields.BUCKETS.getPreferredName(), frequentSets);
        }

        /*
        builder.startObject("frequencies_debug");
        List<Tuple<String, Long>> itemSetsSorted = itemSets.entrySet()
            .stream()
            .map(e -> { return new Tuple<String, Long>(e.getKey(), e.getValue()); })
            .sorted((e1, e2) -> e2.v2().compareTo(e1.v2()))
            .collect(Collectors.toList());

        for (Tuple<String, Long> entry : itemSetsSorted) {
            builder.field(entry.v1(), entry.v2());
        }
        builder.endObject();
        */
        return builder;
    }

    // todo: implements Writeable
    static class FrequentItemSet implements ToXContent {

        private final Map<String, List<String>> items;
        private final long docCount;
        private final double support;

        FrequentItemSet(Map<String, List<String>> items, long docCount, double support) {
            this.items = items;
            this.docCount = docCount;
            this.support = support;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startObject(CommonFields.KEY.getPreferredName());
            for (Entry<String, List<String>> item : items.entrySet()) {
                // TODO: flatten single lists?
                builder.field(item.getKey(), item.getValue());
            }
            builder.endObject();

            builder.field(CommonFields.DOC_COUNT.getPreferredName(), docCount);
            builder.field("support", support);
            builder.endObject();
            return builder;
        }

        public long getDocCount() {
            return docCount;
        }

        public double getSupport() {
            return support;
        }

        public Map<String, List<String>> getItems() {
            return items;
        }

    }

    private void apriori() throws IOException {
        final long relativeStartNanos = System.nanoTime();
        final long totalTransactionCount = transactionStore.getTotalTransactionCount();

        TopItemIds topIds = null;
        TopTransactionIds topTransactionIds = null;
        List<FrequentItemSet> candidateSets = new ArrayList<>();

        try {
            // top items
            topIds = transactionStore.getTopItemIds();

            // top transactions
            topTransactionIds = transactionStore.getTopTransactionIds();

            // start set
            List<Tuple<List<Long>, Long>> startIteration = new ArrayList<>();
            for (Long id : topIds) {
                startIteration.add(new Tuple<>(List.of(id), transactionStore.getItemCount(id)));
            }

            List<Tuple<List<Long>, Long>> lastIteration = startIteration;

            long minCount = (long) (minimumSupport * totalTransactionCount);
            logger.info("total transaction count {}, min count: {}", totalTransactionCount, minCount);
            logger.info("start iteration has {} items", startIteration.size());

            for (int i = 0; i < maxSetSize; ++i) {
                List<Tuple<List<Long>, Long>> newIteration = new ArrayList<>();

                // logger.info("start new iteration");
                Set<String> lookedAt = new HashSet<>();

                for (Tuple<List<Long>, Long> entry : lastIteration) {
                    boolean addAsClosedSet = entry.v1().size() >= minimumSetSize;

                    // try to add new ids
                    for (Long id : topIds) {
                        // if the item is already in the list skip over
                        if (entry.v1().contains(id)) {
                            continue;
                        }

                        // lets add the item
                        List<Long> newItemSet = new ArrayList<>(entry.v1());
                        newItemSet.add(id);

                        // sort for lookup and deduplication
                        Collections.sort(newItemSet, Comparator.comparingLong(transactionStore::getItemCount).reversed());

                        String newItemSetKey = Strings.collectionToDelimitedString(newItemSet, "#");
                        if (lookedAt.add(newItemSetKey) == false) {
                            continue;
                        }

                        long occurences = 0;
                        long countDown = totalTransactionCount;

                        // calculate the new support for it
                        for (Long transactionId : topTransactionIds) {
                            long transActionCount = transactionStore.getTransactionCount(transactionId);

                            if (transactionStore.transactionContainAllIds(newItemSet, transactionId)) {
                                occurences += transActionCount;
                            }
                            countDown -= transActionCount;

                            // exit early if min support can't be reached
                            if (countDown + occurences < minCount) {
                                break;
                            }
                        }

                        double support = (double) occurences / totalTransactionCount;
                        if (support > minimumSupport) {
                            // logger.info("add item to forward set " + newItemSetKey + " occurences: " + occurences + " old: " +
                            // entry.v2());

                            // if the new set has the same count, don't add it to closed sets

                            if (occurences >= entry.v2()) {
                                assert occurences == entry.v2() : "a grown item set can't have more occurences";
                                addAsClosedSet = false;
                            }

                            newIteration.add(new Tuple<List<Long>, Long>(newItemSet, occurences));
                        } else {
                            // logger.info("drop " + newItemSetKey + " support: " + support);
                        }
                    }
                    if (addAsClosedSet) {
                        // logger.info("add to closed set: " + entry);
                        candidateSets.add(toFrequentItemSet(totalTransactionCount, entry.v1(), entry.v2()));
                    }

                }
                lastIteration = newIteration;

            }

            for (Tuple<List<Long>, Long> entry : lastIteration) {
                candidateSets.add(toFrequentItemSet(totalTransactionCount, entry.v1(), entry.v2()));
            }

            candidateSets.sort((e1, e2) -> {
                int diff = Long.compare(e2.getDocCount(), e1.getDocCount());

                // if 2 sets have the same doc count, prefer the larger over the smaller
                if (diff == 0) {
                    diff = e2.getItems().size() - e1.getItems().size();
                    // TODO: if 2 sets have even the same size, do we need to compare the elements?
                }

                return diff;
            });
        } finally {
            Releasables.close(topIds, topTransactionIds);
        }
        // because apriori goes many ways to build sets, we have to do some additional pruning to get to closed sets
        List<FrequentItemSet> closedSets = new ArrayList<>();

        FrequentItemSet last = null;
        for (FrequentItemSet e : candidateSets) {
            if (last != null && last.getItems().entrySet().containsAll(e.getItems().entrySet())) {
                continue;
            }
            closedSets.add(e);

            last = e;
        }

        List<FrequentItemSet> setThatShareTheSameDocCount = new ArrayList<>();

        for (FrequentItemSet e : candidateSets) {
            if (setThatShareTheSameDocCount.isEmpty() || e.getDocCount() != setThatShareTheSameDocCount.get(0).getDocCount()) {
                setThatShareTheSameDocCount.clear();
                setThatShareTheSameDocCount.add(e);
                closedSets.add(e);
                continue;
            }

            boolean foundSuperSet = false;
            for (FrequentItemSet otherItem : setThatShareTheSameDocCount) {
                if (otherItem.getItems().entrySet().containsAll(e.getItems().entrySet())) {
                    foundSuperSet = true;
                    break;
                }
            }

            if (foundSuperSet == false) {
                setThatShareTheSameDocCount.add(e);
                closedSets.add(e);
            }
        }

        this.frequentSets = closedSets;
        final long relativeEndNanos = System.nanoTime();

        logger.info("apriori runtime {} ms", TimeUnit.NANOSECONDS.toMillis(relativeEndNanos - relativeStartNanos));
    }

    private FrequentItemSet toFrequentItemSet(long totalItemCount, List<Long> items, long count) throws IOException {
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
        return new FrequentItemSet(frequentItemsKeyValues, count, (double) count / totalItemCount);
    }

}
