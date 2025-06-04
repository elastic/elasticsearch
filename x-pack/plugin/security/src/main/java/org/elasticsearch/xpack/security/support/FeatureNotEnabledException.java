/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.rest.RestStatus;

public class FeatureNotEnabledException extends ElasticsearchException {

    public static final String DISABLED_FEATURE_METADATA = "es.disabled.feature";

    /**
     * The features names here are constants that form part of our API contract.
     * Callers (e.g. Kibana) may be dependent on these strings. Do not change them without consideration of BWC.
     */
    public enum Feature {
        TOKEN_SERVICE("security_tokens"),
        API_KEY_SERVICE("api_keys");

        private final String featureName;

        Feature(String featureName) {
            this.featureName = featureName;
        }
    }

    @SuppressWarnings("this-escape")
    public FeatureNotEnabledException(Feature feature, String message, Object... args) {
        super(message, args);
        addMetadata(DISABLED_FEATURE_METADATA, feature.featureName);
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }
}
