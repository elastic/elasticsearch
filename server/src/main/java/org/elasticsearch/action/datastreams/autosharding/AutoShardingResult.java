/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.datastreams.autosharding;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

/**
 * Represents an auto sharding recommendation. It includes the current and target number of shards together with a remaining cooldown
 * period that needs to lapse before the current recommendation should be applied.
 * <p>
 * If auto sharding is not applicable for a data stream (e.g. due to
 * {@link DataStreamAutoShardingService#DATA_STREAMS_AUTO_SHARDING_EXCLUDES_SETTING}) the target number of shards will be -1 and cool down
 * remaining {@link TimeValue#MAX_VALUE}.
 */
public record AutoShardingResult(
    AutoShardingType type,
    int currentNumberOfShards,
    int targetNumberOfShards,
    TimeValue coolDownRemaining,
    @Nullable Double writeLoad
) {
    public static final AutoShardingResult NOT_APPLICABLE_RESULT = new AutoShardingResult(
        AutoShardingType.NOT_APPLICABLE,
        -1,
        -1,
        TimeValue.MAX_VALUE,
        null
    );

}
