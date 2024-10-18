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
 * Determines if reserved state should be processed, given the current, already processed version and the candidate
 * new version.
 */
enum ReservedStateVersionCheck implements BiPredicate<Long, Long> {
    /**
     * `true` iff the current version is less than or equal to the next version.
     * This means re-processing the same version. This is the   
     */
    SAME_OR_NEW_VERSION {
        @Override
        public boolean test(Long currentVersion, Long nextVersion) {
            return currentVersion <= nextVersion;
        }
    },
    /**
     * `true` iff the current version is less than the next version. This is the default behavior
     */
    ONLY_NEW_VERSION {
        @Override
        public boolean test(Long currentVersion, Long nextVersion) {
            return currentVersion < nextVersion;
        }
    };
}
