/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.action.datastreams.autosharding.AutoShardingType;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class AutoShardConditionTests extends AbstractWireSerializingTestCase<AutoShardCondition> {

    @Override
    protected Writeable.Reader<AutoShardCondition> instanceReader() {
        return AutoShardCondition::new;
    }

    @Override
    protected AutoShardCondition createTestInstance() {
        return new AutoShardCondition(
            new IncreaseShardsDetails(
                randomFrom(AutoShardingType.INCREASE_SHARDS, AutoShardingType.COOLDOWN_PREVENTED_INCREASE),
                randomNonNegativeInt(),
                randomNonNegativeInt(),
                TimeValue.ZERO,
                randomDoubleBetween(0.0, 300.0, true)
            )
        );
    }

    @Override
    protected AutoShardCondition mutateInstance(AutoShardCondition instance) throws IOException {
        var type = instance.value.type();
        var numberOfShards = instance.value.currentNumberOfShards();
        var targetNumberOfShards = instance.value.targetNumberOfShards();
        var cooldown = instance.value.coolDownRemaining();
        var writeLoad = instance.value.writeLoad();
        switch (randomInt(4)) {
            case 0 -> type = randomValueOtherThan(
                type,
                () -> randomFrom(AutoShardingType.INCREASE_SHARDS, AutoShardingType.COOLDOWN_PREVENTED_INCREASE)
            );
            case 1 -> numberOfShards++;
            case 2 -> targetNumberOfShards++;
            case 3 -> {
                if (type.equals(AutoShardingType.INCREASE_SHARDS)) {
                    type = AutoShardingType.COOLDOWN_PREVENTED_INCREASE;
                }
                cooldown = TimeValue.timeValueMillis(randomNonNegativeLong());
            }
            case 4 -> writeLoad = randomValueOtherThan(writeLoad, () -> randomDoubleBetween(0.0, 500.0, true));
        }
        return new AutoShardCondition(new IncreaseShardsDetails(type, numberOfShards, targetNumberOfShards, cooldown, writeLoad));
    }
}
