/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.async;

import java.util.Map;

/**
 * A task that supports asynchronous execution and provides information necessary for safe temporary storage of results
 */
public interface AsyncTask {
    /**
     * Returns all of the request contexts headers
     */
    Map<String, String> getOriginHeaders();

    /**
     * Returns the {@link AsyncExecutionId} of the task
     */
    AsyncExecutionId getExecutionId();
}
