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

/**
 * A plugin that allows creating custom {@code RecoveryPlannerService}. Only one plugin of this type
 * is allowed to be installed at once.
 */
public interface RecoveryPlannerPlugin {
    RecoveryPlannerService createRecoveryPlannerService(ShardSnapshotsService shardSnapshotsService);
}
