/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ilm;

import org.elasticsearch.action.admin.indices.shrink.ShrinkAction;

import java.util.EnumSet;
import java.util.Locale;

/**
 * Enum representing the different modes that Index Lifecycle Service can operate in.
 */
public enum OperationMode {
    /**
     * This represents a state where no policies are executed
     */
    STOPPED {
        @Override
        public boolean isValidChange(OperationMode nextMode) {
            return nextMode == RUNNING;
        }
    },

    /**
     * this represents a state where only sensitive actions (like {@link ShrinkAction}) will be executed
     * until they finish, at which point the operation mode will move to <code>STOPPED</code>.
     */
    STOPPING {
        @Override
        public boolean isValidChange(OperationMode nextMode) {
            return nextMode == RUNNING || nextMode == STOPPED;
        }
    },

    /**
     * Normal operation where all policies are executed as normal.
     */
    RUNNING {
        @Override
        public boolean isValidChange(OperationMode nextMode) {
            return nextMode == STOPPING;
        }
    };

    public abstract boolean isValidChange(OperationMode nextMode);

    static OperationMode fromString(String string) {
        return EnumSet.allOf(OperationMode.class)
            .stream()
            .filter(e -> string.equalsIgnoreCase(e.name()))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException(String.format(Locale.ROOT, "%s is not a valid operation_mode", string)));
    }
}
