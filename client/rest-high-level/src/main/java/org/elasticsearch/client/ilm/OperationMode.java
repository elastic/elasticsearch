/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
        return EnumSet.allOf(OperationMode.class).stream()
            .filter(e -> string.equalsIgnoreCase(e.name())).findFirst()
            .orElseThrow(() -> new IllegalArgumentException(String.format(Locale.ROOT, "%s is not a valid operation_mode", string)));
    }
}
