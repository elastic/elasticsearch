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

enum ReservedVersionCheck implements BiPredicate<Long, Long> {
    SAME_OR_NEW_VERSION {
        @Override
        public boolean test(Long currentVersion, Long nextVersion) {
            return currentVersion <= nextVersion;
        }
    },
    ONLY_NEW_VERSION {
        @Override
        public boolean test(Long currentVersion, Long nextVersion) {
            return currentVersion < nextVersion;
        }
    };
}
