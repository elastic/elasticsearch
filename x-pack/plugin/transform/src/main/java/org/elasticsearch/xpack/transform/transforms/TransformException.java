/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.ElasticsearchException;

class TransformException extends ElasticsearchException {
    TransformException(String msg, Throwable cause, Object... args) {
        super(msg, cause, args);
    }
}
