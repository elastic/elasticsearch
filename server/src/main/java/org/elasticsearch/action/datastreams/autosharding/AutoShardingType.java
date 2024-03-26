/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.datastreams.autosharding;

/**
 * Represents the type of recommendation the auto sharding service provided.
 */
public enum AutoShardingType {
    INCREASE_SHARDS,
    DECREASE_SHARDS,
    COOLDOWN_PREVENTED_INCREASE,
    COOLDOWN_PREVENTED_DECREASE,
    NO_CHANGE_REQUIRED,
    NOT_APPLICABLE
}
