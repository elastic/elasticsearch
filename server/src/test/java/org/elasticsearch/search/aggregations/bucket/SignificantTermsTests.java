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

package org.elasticsearch.search.aggregations.bucket;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTermsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.ChiSquare;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.GND;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.JLHScore;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.MutualInformation;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.PercentageScore;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.ScriptHeuristic;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.bucket.terms.IncludeExclude;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregatorFactory.ExecutionMode;

import java.util.SortedSet;
import java.util.TreeSet;

public class SignificantTermsTests extends BaseAggregationTestCase<SignificantTermsAggregationBuilder> {

    private static final String[] executionHints;

    static {
        ExecutionMode[] executionModes = ExecutionMode.values();
        executionHints = new String[executionModes.length];
        for (int i = 0; i < executionModes.length; i++) {
            executionHints[i] = executionModes[i].toString();
        }
    }

    @Override
    protected SignificantTermsAggregationBuilder createTestAggregatorBuilder() {
        String name = randomAlphaOfLengthBetween(3, 20);
        SignificantTermsAggregationBuilder factory = new SignificantTermsAggregationBuilder(name, null);
        String field = randomAlphaOfLengthBetween(3, 20);
        randomFieldOrScript(factory, field);

        if (randomBoolean()) {
            factory.missing("MISSING");
        }
        if (randomBoolean()) {
            factory.bucketCountThresholds().setRequiredSize(randomIntBetween(1, Integer.MAX_VALUE));

        }
        if (randomBoolean()) {
            factory.bucketCountThresholds().setShardSize(randomIntBetween(1, Integer.MAX_VALUE));
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
            }
            factory.bucketCountThresholds().setMinDocCount(minDocCount);
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
            factory.bucketCountThresholds().setShardMinDocCount(shardMinDocCount);
        }
        if (randomBoolean()) {
            factory.executionHint(randomFrom(executionHints));
        }
        if (randomBoolean()) {
            factory.format("###.##");
        }
        if (randomBoolean()) {
            IncludeExclude incExc = getIncludeExclude();
            factory.includeExclude(incExc);
        }
        if (randomBoolean()) {
            SignificanceHeuristic significanceHeuristic = getSignificanceHeuristic();
            factory.significanceHeuristic(significanceHeuristic);
        }
        if (randomBoolean()) {
            factory.backgroundFilter(QueryBuilders.termsQuery("foo", "bar"));
        }
        return factory;
    }

    static SignificanceHeuristic getSignificanceHeuristic() {
        SignificanceHeuristic significanceHeuristic = null;
        switch (randomInt(5)) {
        case 0:
            significanceHeuristic = new PercentageScore();
            break;
        case 1:
            significanceHeuristic = new ChiSquare(randomBoolean(), randomBoolean());
            break;
        case 2:
            significanceHeuristic = new GND(randomBoolean());
            break;
        case 3:
            significanceHeuristic = new MutualInformation(randomBoolean(), randomBoolean());
            break;
        case 4:
            significanceHeuristic = new ScriptHeuristic(mockScript("foo"));
            break;
        case 5:
            significanceHeuristic = new JLHScore();
            break;
        default:
            fail();
        }
        return significanceHeuristic;
    }

    static IncludeExclude getIncludeExclude() {
        IncludeExclude incExc = null;
        switch (randomInt(5)) {
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
        default:
            fail();
        }
        return incExc;
    }

}
