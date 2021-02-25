/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.search.action;

import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

public class ClosePointInTimeResponse extends ClearScrollResponse {
    public ClosePointInTimeResponse(boolean succeeded, int numFreed) {
        super(succeeded, numFreed);
    }

    public ClosePointInTimeResponse(StreamInput in) throws IOException {
        super(in);
    }
}
