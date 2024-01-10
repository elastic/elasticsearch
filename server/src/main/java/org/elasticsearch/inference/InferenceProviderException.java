/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.inference;

import org.elasticsearch.ElasticsearchException;

public class InferenceProviderException extends ElasticsearchException {

    public InferenceProviderException(String msg, Throwable cause, Object... args) {
        super(msg, cause, args);
    }
}
