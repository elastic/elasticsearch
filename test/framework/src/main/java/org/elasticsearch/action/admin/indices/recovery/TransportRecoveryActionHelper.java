/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.recovery;

/**
 * Helper methods for {@link TransportRecoveryAction}.
 */
public class TransportRecoveryActionHelper {

    /**
     * Helper method for tests to call {@link TransportRecoveryAction#setOnShardOperation}.
     */
    public static void setOnShardOperation(TransportRecoveryAction transportRecoveryAction, Runnable setOnShardOperation) {
        transportRecoveryAction.setOnShardOperation(setOnShardOperation);
    }
}
