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
    public static final String NAME = "map-reduce-apriori-" + VERSION;

    // TODO: parameterize
    private static final double minSupport = 0.1;
    private static final long minSetSize = 2;
    private static final long maxSetSize = 10;

    private Map<String, Long> itemSets = null;

    private StringBuilder stringBuilder = new StringBuilder();
    private List<Tuple<String, Double>> frequentSets = null;

    public AprioriMapReducer() {}

    public AprioriMapReducer(StreamInput in) throws IOException {
        this.itemSets = in.readMap(StreamInput::readString, StreamInput::readLong);

        // not send over the wire
        this.frequentSets = null;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
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
            AprioriMapReducer apprioriPartition = (AprioriMapReducer) p;
            apprioriPartition.itemSets.forEach((key, value) -> itemSets.merge(key, value, (v1, v2) -> v1 + v2));
        });
    }

    @Override
    public void reduceFinalize() {
        apprioriSimple();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (frequentSets != null) {

            builder.startObject("frequent_sets");
            for (Tuple<String, Double> entry : frequentSets) {
                builder.field(entry.v1(), entry.v2());
            }
            builder.endObject();
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

    private void apprioriSimple() {
        Map<String, Long> frequentItems = new HashMap<>();
        logger.info("appriori simple");
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
        // logger.info("create start set");
        // we iterate on a list of item sets, keys are flattened as in the global list
        List<Tuple<String, Double>> startSet = new ArrayList<>();

        // create a start set with single items that have at least minSupport
        for (Entry<String, Long> entry : frequentItems.entrySet()) {
            double support = entry.getValue().doubleValue() / totalItemCount;
            logger.info("item " + entry.getKey() + " support: " + support);

            if (support > minSupport) {
                startSet.add(new Tuple<>(entry.getKey(), support));
            }
        }

        List<Tuple<String, Double>> lastIteration = startSet;
        List<Tuple<String, Double>> closedSets = new ArrayList<>();

        // frequentSets
        for (int i = 0; i < maxSetSize; ++i) {
            logger.info("run " + i + " with " + lastIteration.size() + "sets");
            Set<String> lookedAt = new HashSet<>();
            List<Tuple<String, Double>> newIteration = new ArrayList<>();

            for (Tuple<String, Double> entry : lastIteration) {
                String[] itemsArray = Strings.tokenizeToStringArray(entry.v1(), "#");
                List<String> items = itemsArray != null ? Arrays.asList(itemsArray) : Collections.singletonList(entry.v1());

                boolean addAsClosedSet = items.size() > minSetSize;
                // iterate over the start set and try to add items
                for (Tuple<String, Double> item : startSet) {

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
                    long minCount = (long) (totalItemCount * minSupport);
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

                    logger.info("Found " + occurences + " for " + newItemSet);

                    double support = (double) occurences / totalItemCount;
                    if (support > minSupport) {
                        logger.info("add item to forward set " + newItemSetKey + " support: " + support);

                        addAsClosedSet = false;
                        newIteration.add(new Tuple<>(newItemSetKey, support));
                    } else {
                        logger.info("drop " + newItemSetKey + " support: " + support);
                    }
                }

                if (addAsClosedSet) {
                    logger.info("add to closed set: " + entry);

                    closedSets.add(entry);
                }

            }
            lastIteration = newIteration;
        }

        closedSets.addAll(lastIteration);
        closedSets.sort((e1, e2) -> e2.v2().compareTo(e1.v2()));

        this.frequentSets = closedSets;
    }
}
