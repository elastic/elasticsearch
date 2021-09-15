/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import static org.hamcrest.Matchers.equalTo;

public class ConditionTests extends ESTestCase {

    public void testMaxAge() {
        final MaxAgeCondition maxAgeCondition = new MaxAgeCondition(TimeValue.timeValueHours(1));

        long indexCreatedMatch = System.currentTimeMillis() - TimeValue.timeValueMinutes(61).getMillis();
        Condition.Result evaluate = maxAgeCondition.evaluate(new Condition.Stats(0, indexCreatedMatch, randomByteSize(), randomByteSize()));
        assertThat(evaluate.condition, equalTo(maxAgeCondition));
        assertThat(evaluate.matched, equalTo(true));

        long indexCreatedNotMatch = System.currentTimeMillis() - TimeValue.timeValueMinutes(59).getMillis();
        evaluate = maxAgeCondition.evaluate(new Condition.Stats(0, indexCreatedNotMatch, randomByteSize(), randomByteSize()));
        assertThat(evaluate.condition, equalTo(maxAgeCondition));
        assertThat(evaluate.matched, equalTo(false));
    }

    public void testMaxDocs() {
        final MaxDocsCondition maxDocsCondition = new MaxDocsCondition(100L);

        long maxDocsMatch = randomIntBetween(100, 1000);
        Condition.Result evaluate = maxDocsCondition.evaluate(new Condition.Stats(maxDocsMatch, 0, randomByteSize(), randomByteSize()));
        assertThat(evaluate.condition, equalTo(maxDocsCondition));
        assertThat(evaluate.matched, equalTo(true));

        long maxDocsNotMatch = randomIntBetween(0, 99);
        evaluate = maxDocsCondition.evaluate(new Condition.Stats(0, maxDocsNotMatch, randomByteSize(), randomByteSize()));
        assertThat(evaluate.condition, equalTo(maxDocsCondition));
        assertThat(evaluate.matched, equalTo(false));
    }

    public void testMaxSize() {
        MaxSizeCondition maxSizeCondition = new MaxSizeCondition(ByteSizeValue.ofMb(randomIntBetween(10, 20)));

        Condition.Result result = maxSizeCondition.evaluate(new Condition.Stats(randomNonNegativeLong(), randomNonNegativeLong(),
            ByteSizeValue.ofMb(0), randomByteSize()));
        assertThat(result.matched, equalTo(false));

        result = maxSizeCondition.evaluate(new Condition.Stats(randomNonNegativeLong(), randomNonNegativeLong(),
            ByteSizeValue.ofMb(randomIntBetween(0, 9)), randomByteSize()));
        assertThat(result.matched, equalTo(false));

        result = maxSizeCondition.evaluate(new Condition.Stats(randomNonNegativeLong(), randomNonNegativeLong(),
            ByteSizeValue.ofMb(randomIntBetween(20, 1000)), randomByteSize()));
        assertThat(result.matched, equalTo(true));
    }

    public void testMaxPrimaryShardSize() {
        MaxPrimaryShardSizeCondition maxPrimaryShardSizeCondition =
            new MaxPrimaryShardSizeCondition(ByteSizeValue.ofMb(randomIntBetween(10, 20)));

        Condition.Result result = maxPrimaryShardSizeCondition.evaluate(new Condition.Stats(randomNonNegativeLong(),
            randomNonNegativeLong(), randomByteSize(), ByteSizeValue.ofMb(0)));
        assertThat(result.matched, equalTo(false));

        result = maxPrimaryShardSizeCondition.evaluate(new Condition.Stats(randomNonNegativeLong(), randomNonNegativeLong(),
            randomByteSize(), ByteSizeValue.ofMb(randomIntBetween(0, 9))));
        assertThat(result.matched, equalTo(false));

        result = maxPrimaryShardSizeCondition.evaluate(new Condition.Stats(randomNonNegativeLong(), randomNonNegativeLong(),
            randomByteSize(), ByteSizeValue.ofMb(randomIntBetween(20, 1000))));
        assertThat(result.matched, equalTo(true));
    }

    public void testEqualsAndHashCode() {
        MaxAgeCondition maxAgeCondition = new MaxAgeCondition(new TimeValue(randomNonNegativeLong()));
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(maxAgeCondition, condition -> new MaxAgeCondition(condition.value),
            condition -> new MaxAgeCondition(new TimeValue(randomNonNegativeLong())));

        MaxDocsCondition maxDocsCondition = new MaxDocsCondition(randomLong());
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(maxDocsCondition, condition -> new MaxDocsCondition(condition.value),
            condition -> new MaxDocsCondition(randomLong()));

        MaxSizeCondition maxSizeCondition = new MaxSizeCondition(randomByteSize());
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(maxSizeCondition, condition -> new MaxSizeCondition(condition.value),
            condition -> new MaxSizeCondition(randomByteSize()));

        MaxPrimaryShardSizeCondition maxPrimaryShardSizeCondition = new MaxPrimaryShardSizeCondition(randomByteSize());
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(maxPrimaryShardSizeCondition,
            condition -> new MaxPrimaryShardSizeCondition(condition.value),
            condition -> new MaxPrimaryShardSizeCondition(randomByteSize()));
    }

    private static ByteSizeValue randomByteSize() {
        return new ByteSizeValue(randomNonNegativeLong());
    }
}
