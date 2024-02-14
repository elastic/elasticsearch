/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.action.datastreams.autosharding.AutoShardingType;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

import static org.elasticsearch.action.datastreams.autosharding.AutoShardingType.COOLDOWN_PREVENTED_INCREASE;
import static org.elasticsearch.action.datastreams.autosharding.AutoShardingType.INCREASE_SHARDS;

/**
 * Our rollover conditions infrastructure need one value that is evaluated to indicate if a condition is met or not.
 * With auto sharding we'd like to include more information as part of the increase shards condition and this class is responsible for
 * encapsulating all the necessary information needed to represent an increase shards condition.
 */
public record IncreaseShardsDetails(
    AutoShardingType type,
    int currentNumberOfShards,
    int targetNumberOfShards,
    TimeValue coolDownRemaining,
    @Nullable Double writeLoad
) {

    public IncreaseShardsDetails {
        if (type.equals(INCREASE_SHARDS) == false && type.equals(COOLDOWN_PREVENTED_INCREASE) == false) {
            throw new IllegalArgumentException("invalid increase shards condition type: " + type);
        }
    }

    boolean isConditionMet() {
        return type.equals(INCREASE_SHARDS);
    }

    @Override
    public String toString() {
        return conditionDisplayValue();
    }

    /**
     * The display value is directly exposed to our users via the {@link Condition#toString()} so we should modify it carefully if needed.
     * It's currently not persisted anywhere but the rollover response displays it.
     */
    // Visible for testing
    String conditionDisplayValue() {
        return "{ type: "
            + type
            + ", currentNumberOfShards: "
            + currentNumberOfShards
            + ", targetNumberOfShards: "
            + targetNumberOfShards
            + ", coolDownRemaining: "
            + coolDownRemaining
            + ", writeLoad: "
            + writeLoad
            + " }";
    }
}
