/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

public enum FailureStrategy {
    RETRY(false, true),
    RETRY_BACKOFF(false, true),
    FAIL_SILENT(false, false),
    FAIL_SEND(true, false);

    private final boolean notifyMaster;
    private final boolean retry;

    FailureStrategy(boolean notifyMaster, boolean retry) {
        this.notifyMaster = notifyMaster;
        this.retry = retry;
    }

    public boolean notifyMaster() {
        return notifyMaster;
    }

    public boolean retry() {
        return retry;
    }
}
