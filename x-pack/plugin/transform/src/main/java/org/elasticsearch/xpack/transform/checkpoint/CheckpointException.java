/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.checkpoint;

import org.elasticsearch.ElasticsearchException;

public class CheckpointException extends ElasticsearchException {
    public CheckpointException(String msg, Object... params) {
        super(msg, params);
    }

    public CheckpointException(String msg, Throwable cause, Object... params) {
        super(msg, cause, params);
    }
}
