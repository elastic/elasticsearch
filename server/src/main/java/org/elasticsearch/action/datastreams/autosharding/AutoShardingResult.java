/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.datastreams.autosharding;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;

import java.util.Arrays;

import static org.elasticsearch.action.datastreams.autosharding.AutoShardingType.COOLDOWN_PREVENTED_DECREASE;
import static org.elasticsearch.action.datastreams.autosharding.AutoShardingType.COOLDOWN_PREVENTED_INCREASE;

/**
 * Represents an auto sharding recommendation. It includes the current and target number of shards together with a remaining cooldown
 * period that needs to lapse before the current recommendation should be applied.
 * <p>
 * If auto sharding is not applicable for a data stream (e.g. due to
 * {@link DataStreamAutoShardingService#DATA_STREAMS_AUTO_SHARDING_EXCLUDES_SETTING}) the target number of shards will be -1 and cool down
 * remaining {@link TimeValue#MAX_VALUE}.
 */
public record AutoShardingResult(AutoShardingType type, int currentNumberOfShards, int targetNumberOfShards, TimeValue coolDownRemaining) {

    static final String COOLDOWN_PREVENTING_TYPES = Arrays.toString(
        new AutoShardingType[] { COOLDOWN_PREVENTED_DECREASE, COOLDOWN_PREVENTED_INCREASE }
    );

    public AutoShardingResult {
        if (type.equals(AutoShardingType.INCREASE_SHARDS) || type.equals(AutoShardingType.DECREASE_SHARDS)) {
            if (coolDownRemaining.equals(TimeValue.ZERO) == false) {
                throw new IllegalArgumentException(
                    "The increase/decrease shards events must have a cooldown period of zero. Use one of ["
                        + COOLDOWN_PREVENTING_TYPES
                        + "] types indead"
                );
            }
        }
    }

    public static final AutoShardingResult NOT_APPLICABLE_RESULT = new AutoShardingResult(
        AutoShardingType.NOT_APPLICABLE,
        -1,
        -1,
        TimeValue.MAX_VALUE
    );

    @Override
    public String toString() {
        return switch (type) {
            case INCREASE_SHARDS -> Strings.format(
                "Recommendation to increase shards from %d to %d",
                currentNumberOfShards,
                targetNumberOfShards
            );
            case DECREASE_SHARDS -> Strings.format(
                "Recommendation to decrease shards from %d to %d",
                currentNumberOfShards,
                targetNumberOfShards
            );
            case COOLDOWN_PREVENTED_INCREASE -> Strings.format(
                "Deferred recommendation to increase shards from %d to %d after cooldown period %s",
                currentNumberOfShards,
                targetNumberOfShards,
                coolDownRemaining
            );
            case COOLDOWN_PREVENTED_DECREASE -> Strings.format(
                "Deferred recommendation to decrease shards from %d to %d after cooldown period %s",
                currentNumberOfShards,
                targetNumberOfShards,
                coolDownRemaining
            );
            case NO_CHANGE_REQUIRED -> Strings.format("Recommendation to leave shards unchanged at %d", currentNumberOfShards);
            case NOT_APPLICABLE -> "No recommendation as auto-sharding not enabled";
        };
    }
}
