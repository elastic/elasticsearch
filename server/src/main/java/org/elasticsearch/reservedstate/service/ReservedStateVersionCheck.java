/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reservedstate.service;

import java.util.function.BiPredicate;

/**
 * Enum representing the logic for determining whether a reserved state should be processed
 * based on the current and next versions.
 */
public enum ReservedStateVersionCheck implements BiPredicate<Long, Long> {
    /**
     * Returns {@code true} if the current version is less than the next version.
     * This is the default behavior when processing changes to file settings.
     */
    ONLY_NEW_VERSION {
        @Override
        public boolean test(Long currentVersion, Long nextVersion) {
            return currentVersion < nextVersion;
        }
    },
    /**
     * Returns {@code true} if the current version is less than or equal to the next version.
     * This allows re-processing of the same version.
     * Used when processing file settings during service startup.
     */
    SAME_OR_NEW_VERSION {
        @Override
        public boolean test(Long currentVersion, Long nextVersion) {
            return currentVersion <= nextVersion;
        }
    }
}
