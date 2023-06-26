/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.RegExp;
import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.search.aggregations.bucket.terms.IncludeExclude;
import org.elasticsearch.search.aggregations.bucket.terms.RareTermsAggregationBuilder;

import java.util.SortedSet;
import java.util.TreeSet;

public class RareTermsTests extends BaseAggregationTestCase<RareTermsAggregationBuilder> {

    @Override
    protected RareTermsAggregationBuilder createTestAggregatorBuilder() {
        String name = randomAlphaOfLengthBetween(3, 20);
        RareTermsAggregationBuilder factory = new RareTermsAggregationBuilder(name);
        String field = randomAlphaOfLengthBetween(3, 20);
        randomFieldOrScript(factory, field);
        if (randomBoolean()) {
            factory.missing("MISSING");
        }
        if (randomBoolean()) {
            factory.format("###.##");
        }
        if (randomBoolean()) {
            IncludeExclude incExc = null;
            switch (randomInt(6)) {
                case 0:
                    incExc = new IncludeExclude(new RegExp("foobar"), null);
                    break;
                case 1:
                    incExc = new IncludeExclude(null, new RegExp("foobaz"));
                    break;
                case 2:
                    incExc = new IncludeExclude(new RegExp("foobar"), new RegExp("foobaz"));
                    break;
                case 3:
                    SortedSet<BytesRef> includeValues = new TreeSet<>();
                    int numIncs = randomIntBetween(1, 20);
                    for (int i = 0; i < numIncs; i++) {
                        includeValues.add(new BytesRef(randomAlphaOfLengthBetween(1, 30)));
                    }
                    SortedSet<BytesRef> excludeValues = null;
                    incExc = new IncludeExclude(includeValues, excludeValues);
                    break;
                case 4:
                    SortedSet<BytesRef> includeValues2 = null;
                    SortedSet<BytesRef> excludeValues2 = new TreeSet<>();
                    int numExcs2 = randomIntBetween(1, 20);
                    for (int i = 0; i < numExcs2; i++) {
                        excludeValues2.add(new BytesRef(randomAlphaOfLengthBetween(1, 30)));
                    }
                    incExc = new IncludeExclude(includeValues2, excludeValues2);
                    break;
                case 5:
                    SortedSet<BytesRef> includeValues3 = new TreeSet<>();
                    int numIncs3 = randomIntBetween(1, 20);
                    for (int i = 0; i < numIncs3; i++) {
                        includeValues3.add(new BytesRef(randomAlphaOfLengthBetween(1, 30)));
                    }
                    SortedSet<BytesRef> excludeValues3 = new TreeSet<>();
                    int numExcs3 = randomIntBetween(1, 20);
                    for (int i = 0; i < numExcs3; i++) {
                        excludeValues3.add(new BytesRef(randomAlphaOfLengthBetween(1, 30)));
                    }
                    incExc = new IncludeExclude(includeValues3, excludeValues3);
                    break;
                case 6:
                    final int numPartitions = randomIntBetween(1, 100);
                    final int partition = randomIntBetween(0, numPartitions - 1);
                    incExc = new IncludeExclude(partition, numPartitions);
                    break;
                default:
                    fail();
            }
            factory.includeExclude(incExc);
        }
        return factory;
    }

}
