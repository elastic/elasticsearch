/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.action.datastreams.autosharding.AutoShardingResult;
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
            new AutoShardingResult(
                randomFrom(AutoShardingType.INCREASE_SHARDS, AutoShardingType.DECREASE_SHARDS),
                randomNonNegativeInt(),
                randomNonNegativeInt(),
                TimeValue.ZERO,
                randomDoubleBetween(0.0, 300.0, true)
            )
        );
    }

    @Override
    protected AutoShardCondition mutateInstance(AutoShardCondition instance) throws IOException {
        var type = instance.autoShardingResult().type();
        var numberOfShards = instance.autoShardingResult().currentNumberOfShards();
        var targetNumberOfShards = instance.autoShardingResult().targetNumberOfShards();
        var writeLoad = instance.autoShardingResult().writeLoad();
        switch (randomInt(3)) {
            case 0 -> type = randomValueOtherThan(
                type,
                () -> randomFrom(AutoShardingType.INCREASE_SHARDS, AutoShardingType.DECREASE_SHARDS)
            );
            case 1 -> numberOfShards++;
            case 2 -> targetNumberOfShards++;
            case 3 -> writeLoad = randomValueOtherThan(writeLoad, () -> randomDoubleBetween(0.0, 500.0, true));
        }
        return new AutoShardCondition(new AutoShardingResult(type, numberOfShards, targetNumberOfShards, TimeValue.ZERO, writeLoad));
    }
}
