/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.checkpoint;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

public class CheckpointException extends ElasticsearchException {
    public CheckpointException(String msg, Object... params) {
        super(msg, null, params);
    }

    public CheckpointException(String msg, Throwable cause, Object... params) {
        super(msg, cause, params);
    }

    public CheckpointException(StreamInput in) throws IOException {
        super(in);
    }
}
