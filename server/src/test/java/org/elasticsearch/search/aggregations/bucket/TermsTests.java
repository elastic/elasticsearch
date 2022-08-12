/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.IncludeExclude;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregatorFactory.ExecutionMode;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

public class TermsTests extends BaseAggregationTestCase<TermsAggregationBuilder> {

    private static final String[] executionHints;

    static {
        ExecutionMode[] executionModes = ExecutionMode.values();
        executionHints = new String[executionModes.length];
        for (int i = 0; i < executionModes.length; i++) {
            executionHints[i] = executionModes[i].toString();
        }
    }

    @Override
    protected TermsAggregationBuilder createTestAggregatorBuilder() {
        String name = randomAlphaOfLengthBetween(3, 20);
        TermsAggregationBuilder factory = new TermsAggregationBuilder(name);
        String field = randomAlphaOfLengthBetween(3, 20);
        randomFieldOrScript(factory, field);
        if (randomBoolean()) {
            factory.missing("MISSING");
        }
        if (randomBoolean()) {
            factory.size(randomIntBetween(1, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            factory.shardSize(randomIntBetween(1, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            int minDocCount = randomInt(4);
            switch (minDocCount) {
                case 0:
                    break;
                case 1:
                case 2:
                case 3:
                case 4:
                    minDocCount = randomIntBetween(0, Integer.MAX_VALUE);
                    break;
                default:
                    fail();
            }
            factory.minDocCount(minDocCount);
        }
        if (randomBoolean()) {
            int shardMinDocCount = randomInt(4);
            switch (shardMinDocCount) {
                case 0:
                    break;
                case 1:
                case 2:
                case 3:
                case 4:
                    shardMinDocCount = randomIntBetween(0, Integer.MAX_VALUE);
                    break;
                default:
                    fail();
            }
            factory.shardMinDocCount(shardMinDocCount);
        }
        if (randomBoolean()) {
            factory.collectMode(randomFrom(SubAggCollectionMode.values()));
        }
        if (randomBoolean()) {
            factory.executionHint(randomFrom(executionHints));
        }
        if (randomBoolean()) {
            factory.format("###.##");
        }
        if (randomBoolean()) {
            String includeRegexp = null, excludeRegexp = null;
            SortedSet<BytesRef> includeValues = null, excludeValues = null;
            boolean hasIncludeOrExclude = false;

            if (randomBoolean()) {
                hasIncludeOrExclude = true;
                if (randomBoolean()) {
                    includeRegexp = randomAlphaOfLengthBetween(5, 10);
                } else {
                    includeValues = new TreeSet<>();
                    int numIncs = randomIntBetween(1, 20);
                    for (int i = 0; i < numIncs; i++) {
                        includeValues.add(new BytesRef(randomAlphaOfLengthBetween(1, 30)));
                    }
                }
            }

            if (randomBoolean()) {
                hasIncludeOrExclude = true;
                if (randomBoolean()) {
                    excludeRegexp = randomAlphaOfLengthBetween(5, 10);
                } else {
                    excludeValues = new TreeSet<>();
                    int numIncs = randomIntBetween(1, 20);
                    for (int i = 0; i < numIncs; i++) {
                        excludeValues.add(new BytesRef(randomAlphaOfLengthBetween(1, 30)));
                    }
                }
            }

            IncludeExclude incExc;
            if (hasIncludeOrExclude) {
                incExc = new IncludeExclude(includeRegexp, excludeRegexp, includeValues, excludeValues);
            } else {
                final int numPartitions = randomIntBetween(1, 100);
                final int partition = randomIntBetween(0, numPartitions - 1);
                incExc = new IncludeExclude(partition, numPartitions);
            }
            factory.includeExclude(incExc);
        }
        if (randomBoolean()) {
            List<BucketOrder> order = randomOrder();
            if (order.size() == 1 && randomBoolean()) {
                factory.order(order.get(0));
            } else {
                factory.order(order);
            }
        }
        if (randomBoolean()) {
            factory.showTermDocCountError(randomBoolean());
        }
        return factory;
    }

    private List<BucketOrder> randomOrder() {
        List<BucketOrder> orders = new ArrayList<>();
        switch (randomInt(4)) {
            case 0 -> orders.add(BucketOrder.key(randomBoolean()));
            case 1 -> orders.add(BucketOrder.count(randomBoolean()));
            case 2 -> orders.add(BucketOrder.aggregation(randomAlphaOfLengthBetween(3, 20), randomBoolean()));
            case 3 -> orders.add(
                BucketOrder.aggregation(randomAlphaOfLengthBetween(3, 20), randomAlphaOfLengthBetween(3, 20), randomBoolean())
            );
            case 4 -> {
                int numOrders = randomIntBetween(1, 3);
                for (int i = 0; i < numOrders; i++) {
                    orders.addAll(randomOrder());
                }
            }
            default -> fail();
        }
        return orders;
    }

}
