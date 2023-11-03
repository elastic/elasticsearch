/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.retry;

import org.elasticsearch.core.TimeValue;

public class RetrySettings {
    private final int maxRetries;
    private final TimeValue retryWaitTime;

    public RetrySettings(int maxRetries, TimeValue retryWaitTime) {
        this.maxRetries = maxRetries;
        this.retryWaitTime = retryWaitTime;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public TimeValue getRetryWaitTime() {
        return retryWaitTime;
    }
}
