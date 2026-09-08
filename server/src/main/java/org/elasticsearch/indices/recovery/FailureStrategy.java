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
    FAIL_SILENT(false),
    FAIL_SEND(true);

    private final boolean notifyMaster;

    FailureStrategy(boolean notifyMaster) {
        this.notifyMaster = notifyMaster;
    }

    public boolean notifyMaster() {
        return notifyMaster;
    }
}
