/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

/**
 * The CheckTargetShardsCountStep requires for an action to provide the target number of shards. Any action
 * implementing this interface can have the CheckTargetShardsCountStep step.
 */
public interface WithTargetNumberOfShards {

    Integer getNumberOfShards();
}
