/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.get;

import org.elasticsearch.ExceptionsHelper;

public final class GetResultOrFailure {
    private final GetResult result;
    private final Exception failure;

    public GetResultOrFailure(GetResult result) {
        this.result = result;
        this.failure = null;
    }

    public GetResultOrFailure(Exception failure) {
        this.result = null;
        this.failure = failure;
    }

    public GetResult getResult() {
        return result;
    }

    public Exception getFailure() {
        return failure;
    }

    public GetResult getResultOrThrow() {
        if (failure != null) {
            throw ExceptionsHelper.convertToRuntime(failure);
        }
        return result;
    }
}
