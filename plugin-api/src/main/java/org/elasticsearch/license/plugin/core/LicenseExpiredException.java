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
public class LicenseExpiredException extends ElasticsearchException {

    private final String feature;

    public LicenseExpiredException(String feature) {
        super("license expired for feature [" + feature + "]");
        this.feature = feature;
    }

    @Override
    public RestStatus status() {
        return RestStatus.UNAUTHORIZED;
    }

    public String feature() {
        return feature;
    }
}
