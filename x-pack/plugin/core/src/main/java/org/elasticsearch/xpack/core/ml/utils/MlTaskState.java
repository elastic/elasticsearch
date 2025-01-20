/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.utils;

import org.elasticsearch.core.Nullable;

import java.time.Instant;

public interface MlTaskState {

    /**
     * The time of the last state change.
     */
    @Nullable
    Instant getLastStateChangeTime();

    /**
     * @return Is the task in the <code>failed</code> state?
     */
    boolean isFailed();
}
