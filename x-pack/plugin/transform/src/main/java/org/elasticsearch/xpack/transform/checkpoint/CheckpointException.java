/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.checkpoint;

import org.elasticsearch.ElasticsearchException;

class CheckpointException extends ElasticsearchException {
    CheckpointException(String msg, Object... params) {
        super(msg, null, params);
    }

    CheckpointException(String msg, Throwable cause, Object... params) {
        super(msg, cause, params);
    }
}
