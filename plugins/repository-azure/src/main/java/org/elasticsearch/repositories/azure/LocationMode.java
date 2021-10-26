/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.azure;

enum LocationMode {
    PRIMARY_ONLY,
    SECONDARY_ONLY,
    PRIMARY_THEN_SECONDARY,
    SECONDARY_THEN_PRIMARY;

    boolean isSecondary() {
        return this == SECONDARY_ONLY || this == SECONDARY_THEN_PRIMARY;
    }
}
