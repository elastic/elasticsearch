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
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.aggregations.Aggregation.CommonFields;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.ml.aggs.mapreduce.MapReducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AprioriMapReducer implements MapReducer {
    private static final Logger logger = LogManager.getLogger(AprioriMapReducer.class);

    private static final int VERSION = 1;
    public static final String NAME = "frequent_items-apriori-" + VERSION;

    // TODO: do we need this parameter???
    private static final long maxSetSize = 10;

    private final String aggregationWritableName;

    private Map<String, Long> itemSets = null;
    private final double minimumSupport;
    private final int minimumSetSize;
    private final int size;

    private StringBuilder stringBuilder = new StringBuilder();
    private List<FrequentItemSet> frequentSets = null;

    public AprioriMapReducer(String aggregationWritableName, double minimumSupport, int minimumSetSize, int size) {
        this.aggregationWritableName = aggregationWritableName;
        this.minimumSupport = minimumSupport;
        this.minimumSetSize = minimumSetSize;
        this.size = size;
    }

    public AprioriMapReducer(StreamInput in) throws IOException {
        this.aggregationWritableName = in.readString();

        // parameters
        this.minimumSupport = in.readDouble();
        this.minimumSetSize = in.readInt();
        this.size = in.readInt();

        // data
        this.itemSets = in.readMap(StreamInput::readString, StreamInput::readLong);

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
        out.writeMap(itemSets, StreamOutput::writeString, StreamOutput::writeLong);
    }

    @Override
    public void mapInit() {
        itemSets = new HashMap<>();
    }

    @Override
    public void map(Stream<Tuple<String, List<Object>>> keyValues) {

        // dump encoding:
        // key: [value1, value2, value3] -> "key!value1#key!value2#key!value3#"

        stringBuilder.setLength(0);
        keyValues.forEach(v -> {
            v.v2().stream().sorted().forEach(fieldValue -> {
                stringBuilder.append(v.v1());
                stringBuilder.append("!");
                stringBuilder.append(fieldValue);
                stringBuilder.append("#");
            });

        });

        String key = stringBuilder.toString();
        itemSets.compute(key, (k, v) -> (v == null) ? 1 : v + 1);
    }

    @Override
    public void reduce(Stream<MapReducer> partitions) {
        partitions.forEach(p -> {
            // we reuse itemSets, therefore must skip the one we already have
            if (this != p) {
                logger.info("reduce itemSets: " + itemSets.size());
                AprioriMapReducer apprioriPartition = (AprioriMapReducer) p;
                apprioriPartition.itemSets.forEach((key, value) -> itemSets.merge(key, value, (v1, v2) -> v1 + v2));
            }
        });
    }

    @Override
    public void reduceFinalize() {
        aprioriSimple();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (frequentSets != null) {
            builder.field("frequent_sets", frequentSets);
        }

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
            for (Entry<String, List<String>> item : items.entrySet()) {
                // TODO: flatten single lists?
                builder.field(item.getKey(), item.getValue());
            }

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

    private void aprioriSimple() {
        Map<String, Long> frequentItems = new HashMap<>();
        logger.info("apriori simple");
        // TODO; this should be reverse sorted
        List<Tuple<List<String>, Long>> frequentSets = itemSets.entrySet()
            .stream()
            .map(
                e -> { return new Tuple<List<String>, Long>(Arrays.asList(Strings.tokenizeToStringArray(e.getKey(), "#")), e.getValue()); }
            )
            .sorted((e1, e2) -> e2.v2().compareTo(e1.v2()))
            .collect(Collectors.toList());
        // logger.info("1st item count: " + frequentSets.get(0).v2());

        // build a global item frequency list
        long totalItemCount = 0;

        // note: after the global frequency list is build it still contains items which have freq < min_support, we could prune that
        for (Entry<String, Long> entry : itemSets.entrySet()) {
            String[] items = Strings.tokenizeToStringArray(entry.getKey(), "#");
            for (int i = 0; i < items.length; ++i) {
                frequentItems.merge(items[i], entry.getValue(), (v1, v2) -> v1 + v2);
            }
            totalItemCount += entry.getValue();
        }
        logger.info("total item count: " + totalItemCount);
        // logger.info("create start set");
        // we iterate on a list of item sets, keys are flattened as in the global list
        List<Tuple<String, Long>> startSet = new ArrayList<>();

        // create a start set with single items that have at least minSupport
        for (Entry<String, Long> entry : frequentItems.entrySet()) {
            double support = entry.getValue().doubleValue() / totalItemCount;
            // logger.info("item " + entry.getKey() + " support: " + support);

            if (support > minimumSupport) {
                startSet.add(new Tuple<>(entry.getKey(), entry.getValue()));
            }
        }

        List<Tuple<String, Long>> lastIteration = startSet;
        List<FrequentItemSet> candidateSets = new ArrayList<>();

        // frequentSets
        for (int i = 0; i < maxSetSize; ++i) {
            // logger.info("run " + i + " with " + lastIteration.size() + "sets");
            Set<String> lookedAt = new HashSet<>();
            List<Tuple<String, Long>> newIteration = new ArrayList<>();

            for (Tuple<String, Long> entry : lastIteration) {
                String[] itemsArray = Strings.tokenizeToStringArray(entry.v1(), "#");
                List<String> items = itemsArray != null ? Arrays.asList(itemsArray) : Collections.singletonList(entry.v1());

                boolean addAsClosedSet = items.size() > minimumSetSize;
                // iterate over the start set and try to add items
                for (Tuple<String, Long> item : startSet) {

                    // skip if already in the list
                    if (items.contains(item.v1())) {
                        continue;
                    }

                    // lets add the item
                    List<String> newItemSet = new ArrayList<>(items);
                    newItemSet.add(item.v1());

                    // BOW it, and continue if we already considered this one
                    Collections.sort(newItemSet);
                    String newItemSetKey = Strings.collectionToDelimitedString(newItemSet, "#");
                    if (lookedAt.add(newItemSetKey) == false) {
                        continue;
                    }

                    long occurences = 0;
                    long countDown = totalItemCount;
                    long minCount = (long) (totalItemCount * minimumSupport);
                    // calculate the new support for it
                    for (Tuple<List<String>, Long> groundTruth : frequentSets) {
                        // logger.info("Lookup " + newItemSet + " in " + groundTruth.v1());

                        if (groundTruth.v1().containsAll(newItemSet)) {
                            occurences += groundTruth.v2();
                        }
                        countDown -= groundTruth.v2();

                        // exit early if min support can't be reached
                        if (countDown + occurences < minCount) {
                            break;
                        }
                    }

                    // logger.info("Found " + occurences + " for " + newItemSet);

                    double support = (double) occurences / totalItemCount;
                    if (support > minimumSupport) {
                        // logger.info("add item to forward set " + newItemSetKey + " occurences: " + occurences + " old: " + entry.v2());

                        // if the new set has the same count, don't add it to closed sets

                        if (occurences >= entry.v2()) {
                            assert occurences == entry.v2() : "a grown item set can't have more occurences";
                            addAsClosedSet = false;
                        }

                        newIteration.add(new Tuple<>(newItemSetKey, occurences));
                    } else {
                        // logger.info("drop " + newItemSetKey + " support: " + support);
                    }
                }

                if (addAsClosedSet) {
                    // logger.info("add to closed set: " + entry);
                    candidateSets.add(toFrequentItemSet(totalItemCount, entry));
                }

            }
            lastIteration = newIteration;
        }

        for (Tuple<String, Long> item : lastIteration) {
            candidateSets.add(toFrequentItemSet(totalItemCount, item));
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
        this.frequentSets = closedSets;
    }

    private FrequentItemSet toFrequentItemSet(long totalItemCount, Tuple<String, Long> entry) {
        Map<String, List<String>> frequentItemsKeyValues = new HashMap<>();
        String[] closedSetItems = Strings.tokenizeToStringArray(entry.v1(), "#");

        // logger.info("toFrequentItem " + entry.v1());

        for (String keyValue : closedSetItems) {
            String[] closedSetKeyValue = Strings.tokenizeToStringArray(keyValue, "!");
            if (frequentItemsKeyValues.containsKey(closedSetKeyValue[0])) {
                frequentItemsKeyValues.get(closedSetKeyValue[0]).add(closedSetKeyValue[1]);
            } else {
                List<String> l = new ArrayList<>();
                l.add(closedSetKeyValue[1]);
                frequentItemsKeyValues.put(closedSetKeyValue[0], l);
            }
        }
        return new FrequentItemSet(frequentItemsKeyValues, entry.v2(), (double) entry.v2() / totalItemCount);
    }
}
