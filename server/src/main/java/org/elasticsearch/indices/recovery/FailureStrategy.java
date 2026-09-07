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
    ABORT(true, false),
    FAIL_SILENT(false, false),
    FAIL_SEND(false, true);

    private final boolean aborted;
    private final boolean notifyMaster;

    FailureStrategy(boolean aborted, boolean notifyMaster) {
        if (aborted) assert !notifyMaster : "never notify master on aborted recovery";
        this.aborted = aborted;
        this.notifyMaster = notifyMaster;
    }

    public boolean aborted() {
        return aborted;
    }

    public boolean notifyMaster() {
        return notifyMaster;
    }
}
