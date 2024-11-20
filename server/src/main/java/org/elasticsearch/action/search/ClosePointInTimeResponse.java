/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.rest.RestStatus;

import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

public class ClosePointInTimeResponse extends ClearScrollResponse {
    public ClosePointInTimeResponse(boolean succeeded, int numFreed) {
        super(succeeded, numFreed);
    }

    @Override
    public RestStatus status() {
        if (isSucceeded() || getNumFreed() > 0) {
            return OK;
        } else {
            return NOT_FOUND;
        }
    }
}
