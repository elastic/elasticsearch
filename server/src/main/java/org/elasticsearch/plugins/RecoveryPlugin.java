/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins;

import org.elasticsearch.indices.recovery.RecoveryGate;

import java.util.Collection;
import java.util.List;

public interface RecoveryPlugin {

    /// Returns the recovery gates contributed by this plugin. Called once during node construction.
    default Collection<RecoveryGate> getRecoveryGates() {
        return List.of();
    }
}
