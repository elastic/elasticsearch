/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.action.datastreams.autosharding.AutoShardingType;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ConditionTests extends ESTestCase {

    public void testMaxAge() {
        final MaxAgeCondition maxAgeCondition = new MaxAgeCondition(TimeValue.timeValueHours(1));

        long indexCreatedMatch = System.currentTimeMillis() - TimeValue.timeValueMinutes(61).getMillis();
        Condition.Result evaluate = maxAgeCondition.evaluate(
            new Condition.Stats(0, indexCreatedMatch, randomByteSize(), randomByteSize(), randomNonNegativeLong())
        );
        assertThat(evaluate.condition(), equalTo(maxAgeCondition));
        assertThat(evaluate.matched(), equalTo(true));

        long indexCreatedNotMatch = System.currentTimeMillis() - TimeValue.timeValueMinutes(59).getMillis();
        evaluate = maxAgeCondition.evaluate(
            new Condition.Stats(0, indexCreatedNotMatch, randomByteSize(), randomByteSize(), randomNonNegativeLong())
        );
        assertThat(evaluate.condition(), equalTo(maxAgeCondition));
        assertThat(evaluate.matched(), equalTo(false));
    }

    public void testMaxDocs() {
        final MaxDocsCondition maxDocsCondition = new MaxDocsCondition(100L);

        long maxDocsMatch = randomIntBetween(100, 1000);
        Condition.Result evaluate = maxDocsCondition.evaluate(
            new Condition.Stats(maxDocsMatch, 0, randomByteSize(), randomByteSize(), randomNonNegativeLong())
        );
        assertThat(evaluate.condition(), equalTo(maxDocsCondition));
        assertThat(evaluate.matched(), equalTo(true));

        long maxDocsNotMatch = randomIntBetween(0, 99);
        evaluate = maxDocsCondition.evaluate(
            new Condition.Stats(maxDocsNotMatch, 0, randomByteSize(), randomByteSize(), randomNonNegativeLong())
        );
        assertThat(evaluate.condition(), equalTo(maxDocsCondition));
        assertThat(evaluate.matched(), equalTo(false));
    }

    public void testMaxSize() {
        MaxSizeCondition maxSizeCondition = new MaxSizeCondition(ByteSizeValue.ofMb(randomIntBetween(10, 20)));

        Condition.Result result = maxSizeCondition.evaluate(
            new Condition.Stats(
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                ByteSizeValue.ofMb(0),
                randomByteSize(),
                randomNonNegativeLong()
            )
        );
        assertThat(result.matched(), equalTo(false));

        result = maxSizeCondition.evaluate(
            new Condition.Stats(
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                ByteSizeValue.ofMb(randomIntBetween(0, 9)),
                randomByteSize(),
                randomNonNegativeLong()
            )
        );
        assertThat(result.matched(), equalTo(false));

        result = maxSizeCondition.evaluate(
            new Condition.Stats(
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                ByteSizeValue.ofMb(randomIntBetween(20, 1000)),
                randomByteSize(),
                randomNonNegativeLong()
            )
        );
        assertThat(result.matched(), equalTo(true));
    }

    public void testMaxPrimaryShardSize() {
        MaxPrimaryShardSizeCondition maxPrimaryShardSizeCondition = new MaxPrimaryShardSizeCondition(
            ByteSizeValue.ofMb(randomIntBetween(10, 20))
        );

        Condition.Result result = maxPrimaryShardSizeCondition.evaluate(
            new Condition.Stats(
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomByteSize(),
                ByteSizeValue.ofMb(0),
                randomNonNegativeLong()
            )
        );
        assertThat(result.matched(), equalTo(false));

        result = maxPrimaryShardSizeCondition.evaluate(
            new Condition.Stats(
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomByteSize(),
                ByteSizeValue.ofMb(randomIntBetween(0, 9)),
                randomNonNegativeLong()
            )
        );
        assertThat(result.matched(), equalTo(false));

        result = maxPrimaryShardSizeCondition.evaluate(
            new Condition.Stats(
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomByteSize(),
                ByteSizeValue.ofMb(randomIntBetween(20, 1000)),
                randomNonNegativeLong()
            )
        );
        assertThat(result.matched(), equalTo(true));
    }

    public void testMaxPrimaryShardDocs() {
        final MaxPrimaryShardDocsCondition maxPrimaryShardDocsCondition = new MaxPrimaryShardDocsCondition(100L);

        long maxPrimaryShardDocsMatch = randomIntBetween(100, 1000);
        Condition.Result evaluate = maxPrimaryShardDocsCondition.evaluate(
            new Condition.Stats(randomNonNegativeLong(), 0, randomByteSize(), randomByteSize(), maxPrimaryShardDocsMatch)
        );
        assertThat(evaluate.condition(), equalTo(maxPrimaryShardDocsCondition));
        assertThat(evaluate.matched(), equalTo(true));

        long maxPrimaryShardDocsNotMatch = randomIntBetween(0, 99);
        evaluate = maxPrimaryShardDocsCondition.evaluate(
            new Condition.Stats(randomNonNegativeLong(), 0, randomByteSize(), randomByteSize(), maxPrimaryShardDocsNotMatch)
        );
        assertThat(evaluate.condition(), equalTo(maxPrimaryShardDocsCondition));
        assertThat(evaluate.matched(), equalTo(false));
    }

    public void testMinAge() {
        final MinAgeCondition minAgeCondition = new MinAgeCondition(TimeValue.timeValueHours(1));

        long indexCreatedMatch = System.currentTimeMillis() - TimeValue.timeValueMinutes(61).getMillis();
        Condition.Result evaluate = minAgeCondition.evaluate(
            new Condition.Stats(0, indexCreatedMatch, randomByteSize(), randomByteSize(), randomNonNegativeLong())
        );
        assertThat(evaluate.condition(), equalTo(minAgeCondition));
        assertThat(evaluate.matched(), equalTo(true));

        long indexCreatedNotMatch = System.currentTimeMillis() - TimeValue.timeValueMinutes(59).getMillis();
        evaluate = minAgeCondition.evaluate(
            new Condition.Stats(0, indexCreatedNotMatch, randomByteSize(), randomByteSize(), randomNonNegativeLong())
        );
        assertThat(evaluate.condition(), equalTo(minAgeCondition));
        assertThat(evaluate.matched(), equalTo(false));
    }

    public void testMinDocs() {
        final MinDocsCondition minDocsCondition = new MinDocsCondition(100L);

        long minDocsMatch = randomIntBetween(100, 1000);
        Condition.Result evaluate = minDocsCondition.evaluate(
            new Condition.Stats(minDocsMatch, 0, randomByteSize(), randomByteSize(), randomNonNegativeLong())
        );
        assertThat(evaluate.condition(), equalTo(minDocsCondition));
        assertThat(evaluate.matched(), equalTo(true));

        long minDocsNotMatch = randomIntBetween(0, 99);
        evaluate = minDocsCondition.evaluate(
            new Condition.Stats(minDocsNotMatch, 0, randomByteSize(), randomByteSize(), randomNonNegativeLong())
        );
        assertThat(evaluate.condition(), equalTo(minDocsCondition));
        assertThat(evaluate.matched(), equalTo(false));
    }

    public void testMinSize() {
        MinSizeCondition minSizeCondition = new MinSizeCondition(ByteSizeValue.ofMb(randomIntBetween(10, 20)));

        Condition.Result result = minSizeCondition.evaluate(
            new Condition.Stats(
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                ByteSizeValue.ofMb(0),
                randomByteSize(),
                randomNonNegativeLong()
            )
        );
        assertThat(result.matched(), equalTo(false));

        result = minSizeCondition.evaluate(
            new Condition.Stats(
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                ByteSizeValue.ofMb(randomIntBetween(0, 9)),
                randomByteSize(),
                randomNonNegativeLong()
            )
        );
        assertThat(result.matched(), equalTo(false));

        result = minSizeCondition.evaluate(
            new Condition.Stats(
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                ByteSizeValue.ofMb(randomIntBetween(20, 1000)),
                randomByteSize(),
                randomNonNegativeLong()
            )
        );
        assertThat(result.matched(), equalTo(true));
    }

    public void testMinPrimaryShardSize() {
        MinPrimaryShardSizeCondition minPrimaryShardSizeCondition = new MinPrimaryShardSizeCondition(
            ByteSizeValue.ofMb(randomIntBetween(10, 20))
        );

        Condition.Result result = minPrimaryShardSizeCondition.evaluate(
            new Condition.Stats(
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomByteSize(),
                ByteSizeValue.ofMb(0),
                randomNonNegativeLong()
            )
        );
        assertThat(result.matched(), equalTo(false));

        result = minPrimaryShardSizeCondition.evaluate(
            new Condition.Stats(
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomByteSize(),
                ByteSizeValue.ofMb(randomIntBetween(0, 9)),
                randomNonNegativeLong()
            )
        );
        assertThat(result.matched(), equalTo(false));

        result = minPrimaryShardSizeCondition.evaluate(
            new Condition.Stats(
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomByteSize(),
                ByteSizeValue.ofMb(randomIntBetween(20, 1000)),
                randomNonNegativeLong()
            )
        );
        assertThat(result.matched(), equalTo(true));
    }

    public void testMinPrimaryShardDocs() {
        final MinPrimaryShardDocsCondition minPrimaryShardDocsCondition = new MinPrimaryShardDocsCondition(100L);

        long minPrimaryShardDocsMatch = randomIntBetween(100, 1000);
        Condition.Result evaluate = minPrimaryShardDocsCondition.evaluate(
            new Condition.Stats(randomNonNegativeLong(), 0, randomByteSize(), randomByteSize(), minPrimaryShardDocsMatch)
        );
        assertThat(evaluate.condition(), equalTo(minPrimaryShardDocsCondition));
        assertThat(evaluate.matched(), equalTo(true));

        long minPrimaryShardDocsNotMatch = randomIntBetween(0, 99);
        evaluate = minPrimaryShardDocsCondition.evaluate(
            new Condition.Stats(randomNonNegativeLong(), 0, randomByteSize(), randomByteSize(), minPrimaryShardDocsNotMatch)
        );
        assertThat(evaluate.condition(), equalTo(minPrimaryShardDocsCondition));
        assertThat(evaluate.matched(), equalTo(false));
    }

    public void testEqualsAndHashCode() {
        MaxAgeCondition maxAgeCondition = new MaxAgeCondition(new TimeValue(randomNonNegativeLong()));
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            maxAgeCondition,
            condition -> new MaxAgeCondition(condition.value),
            condition -> new MaxAgeCondition(new TimeValue(randomNonNegativeLong()))
        );

        MaxDocsCondition maxDocsCondition = new MaxDocsCondition(randomLong());
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            maxDocsCondition,
            condition -> new MaxDocsCondition(condition.value),
            condition -> new MaxDocsCondition(randomLong())
        );

        MaxSizeCondition maxSizeCondition = new MaxSizeCondition(randomByteSize());
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            maxSizeCondition,
            condition -> new MaxSizeCondition(condition.value),
            condition -> new MaxSizeCondition(randomByteSize())
        );

        MaxPrimaryShardSizeCondition maxPrimaryShardSizeCondition = new MaxPrimaryShardSizeCondition(randomByteSize());
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            maxPrimaryShardSizeCondition,
            condition -> new MaxPrimaryShardSizeCondition(condition.value),
            condition -> new MaxPrimaryShardSizeCondition(randomByteSize())
        );

        MaxPrimaryShardDocsCondition maxPrimaryShardDocsCondition = new MaxPrimaryShardDocsCondition(randomNonNegativeLong());
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            maxPrimaryShardDocsCondition,
            condition -> new MaxPrimaryShardDocsCondition(condition.value),
            condition -> new MaxPrimaryShardDocsCondition(randomNonNegativeLong())
        );

        MinAgeCondition minAgeCondition = new MinAgeCondition(new TimeValue(randomNonNegativeLong()));
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            minAgeCondition,
            condition -> new MinAgeCondition(condition.value),
            condition -> new MinAgeCondition(new TimeValue(randomNonNegativeLong()))
        );

        MinDocsCondition minDocsCondition = new MinDocsCondition(randomLong());
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            minDocsCondition,
            condition -> new MinDocsCondition(condition.value),
            condition -> new MinDocsCondition(randomLong())
        );

        MinSizeCondition minSizeCondition = new MinSizeCondition(randomByteSize());
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            minSizeCondition,
            condition -> new MinSizeCondition(condition.value),
            condition -> new MinSizeCondition(randomByteSize())
        );

        MinPrimaryShardSizeCondition minPrimaryShardSizeCondition = new MinPrimaryShardSizeCondition(randomByteSize());
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            minPrimaryShardSizeCondition,
            condition -> new MinPrimaryShardSizeCondition(condition.value),
            condition -> new MinPrimaryShardSizeCondition(randomByteSize())
        );

        MinPrimaryShardDocsCondition minPrimaryShardDocsCondition = new MinPrimaryShardDocsCondition(randomNonNegativeLong());
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            minPrimaryShardDocsCondition,
            condition -> new MinPrimaryShardDocsCondition(condition.value),
            condition -> new MinPrimaryShardDocsCondition(randomNonNegativeLong())
        );
        AutoShardCondition autoShardCondition = new AutoShardCondition(
            new IncreaseShardsDetails(AutoShardingType.INCREASE_SHARDS, 1, 3, TimeValue.ZERO, 3.0)
        );
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            autoShardCondition,
            condition -> new AutoShardCondition(condition.value),
            condition -> new AutoShardCondition(
                new IncreaseShardsDetails(
                    AutoShardingType.COOLDOWN_PREVENTED_INCREASE,
                    randomNonNegativeInt(),
                    randomNonNegativeInt(),
                    TimeValue.timeValueMillis(randomNonNegativeLong()),
                    5.0
                )
            )
        );
    }

    public void testAutoShardCondition() {
        {
            // condition met
            AutoShardCondition autoShardCondition = new AutoShardCondition(
                new IncreaseShardsDetails(AutoShardingType.INCREASE_SHARDS, 1, 3, TimeValue.ZERO, 3.0)
            );
            assertThat(
                autoShardCondition.evaluate(
                    new Condition.Stats(1, randomNonNegativeLong(), randomByteSizeValue(), randomByteSizeValue(), 1)
                ),
                is(true)
            );
        }

        {
            // condition is not met
            AutoShardCondition autoShardCondition = new AutoShardCondition(
                new IncreaseShardsDetails(
                    AutoShardingType.COOLDOWN_PREVENTED_INCREASE,
                    1,
                    3,
                    TimeValue.timeValueMillis(randomNonNegativeLong()),
                    3.0
                )
            );
            assertThat(
                autoShardCondition.evaluate(
                    new Condition.Stats(1, randomNonNegativeLong(), randomByteSizeValue(), randomByteSizeValue(), 1)
                ),
                is(false)
            );
        }
    }

    public void testAutoShardCondtionXContent() throws IOException {
        AutoShardCondition autoShardCondition = new AutoShardCondition(
            new IncreaseShardsDetails(AutoShardingType.INCREASE_SHARDS, 1, 3, TimeValue.ZERO, 2.0)
        );
        {

            AutoShardCondition parsedCondition = AutoShardCondition.fromXContent(createParser(JsonXContent.jsonXContent, """
                {
                        "type": "INCREASE_SHARDS",
                        "cool_down_remaining": "0s",
                        "current_number_of_shards": 1,
                        "target_number_of_shards": 3,
                         "write_load": 2.0
                    }
                """));
            assertThat(parsedCondition.value, is(autoShardCondition.value));
        }

        {
            // let's test the met_conditions parsing that is part of the rollover_info
            long time = System.currentTimeMillis();
            RolloverInfo info = new RolloverInfo("logs-nginx", List.of(autoShardCondition), time);

            RolloverInfo parsedInfo = RolloverInfo.parse(
                createParser(
                    JsonXContent.jsonXContent,
                    "{\n"
                        + " \"met_conditions\": {\n"
                        + "    \"auto_sharding\": {\n"
                        + "        \"type\": \"INCREASE_SHARDS\",\n"
                        + "        \"cool_down_remaining\": \"0s\",\n"
                        + "        \"current_number_of_shards\": 1,\n"
                        + "        \"target_number_of_shards\": 3,\n"
                        + "         \"write_load\": 2.0\n"
                        + "    }\n"
                        + " },\n"
                        + " \"time\": "
                        + time
                        + "\n"
                        + "        }"
                ),
                "logs-nginx"
            );
            assertThat(parsedInfo, is(info));
        }
    }

    private static ByteSizeValue randomByteSize() {
        return ByteSizeValue.ofBytes(randomNonNegativeLong());
    }
}
