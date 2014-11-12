/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.rest.RestStatus;

/**
 * Exception to be thrown when a feature action requires a valid license
 */
public class ExpiredLicenseException extends ElasticsearchException {

    public ExpiredLicenseException(String feature) {
        super(feature + " license has expired");
    }

    @Override
    public RestStatus status() {
        return RestStatus.UNAUTHORIZED;
    }
}
