/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

enum IndexStatus {
    CLOSED(false),
    UNHEALTHY(false),
    TOO_OLD(false),
    NEEDS_CREATION(true),
    NEEDS_VERSION_BUMP(true),
    UP_TO_DATE(false),
    NEEDS_MAPPINGS_UPDATE(true);

    /**
     * Whether a status is for informational purposes only or whether it should be acted upon and may change cluster state.
     */
    public final boolean actionable;

    IndexStatus(boolean actionable) {
        this.actionable = actionable;
    }
}
