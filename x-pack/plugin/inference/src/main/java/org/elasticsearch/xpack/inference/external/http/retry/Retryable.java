/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.retry;

import org.elasticsearch.xpack.inference.external.request.Request;

/**
 * Provides an interface for determining if an error should be retried and a way to modify
 * the request to based on the type of failure that occurred.
 */
public interface Retryable {
    Request rebuildRequest(Request original);

    boolean shouldRetry();
}
