/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.indices.recovery.plan.RecoveryPlannerService;
import org.elasticsearch.indices.recovery.plan.ShardSnapshotsService;

import java.util.Optional;

/**
 * A plugin that allows creating custom {@code RecoveryPlannerService}. Only one {@code RecoveryPlannerService} is allowed to be active
 * at once.
 */
public interface RecoveryPlannerPlugin {
    Optional<RecoveryPlannerService> createRecoveryPlannerService(ShardSnapshotsService shardSnapshotsService);
}
