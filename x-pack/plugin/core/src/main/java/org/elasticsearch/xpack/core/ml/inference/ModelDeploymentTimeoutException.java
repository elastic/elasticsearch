/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;

public class ModelDeploymentTimeoutException extends ElasticsearchStatusException {
    public ModelDeploymentTimeoutException(String message) {
        super(message, RestStatus.REQUEST_TIMEOUT);
    }
}
