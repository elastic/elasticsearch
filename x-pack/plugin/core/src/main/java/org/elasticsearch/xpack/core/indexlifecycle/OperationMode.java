/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

/**
 * Enum representing the different modes that Index Lifecycle Service can operate in.
 */
public enum OperationMode {
    /**
     * This represents a state where no policies are executed
     */
    MAINTENANCE {
        @Override
        public boolean isValidChange(OperationMode nextMode) {
            return nextMode == NORMAL;
        }
    },

    /**
     * this representes a state where only sensitive actions (like {@link ShrinkAction}) will be executed
     * until they finish, at which point the operation mode will move to maintenance mode.
     */
    MAINTENANCE_REQUESTED {
        @Override
        public boolean isValidChange(OperationMode nextMode) {
            return nextMode == NORMAL || nextMode == MAINTENANCE;
        }
    },

    /**
     * Normal operation where all policies are executed as normal.
     */
    NORMAL {
        @Override
        public boolean isValidChange(OperationMode nextMode) {
            return nextMode == MAINTENANCE_REQUESTED;
        }
    };

    public abstract boolean isValidChange(OperationMode nextMode);
}
