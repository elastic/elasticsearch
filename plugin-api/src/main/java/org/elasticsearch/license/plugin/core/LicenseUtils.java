/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.collect.Tuple;

public class LicenseUtils {

    public final static String EXPIRED_FEATURE_HEADER = "es.license.expired.feature";

    /**
     * Exception to be thrown when a feature action requires a valid license, but license
     * has expired
     *
     * <code>feature</code> accessible through {@link #EXPIRED_FEATURE_HEADER} in the
     * exception's rest header
     */
    public static ElasticsearchException newExpirationException(String feature) {
        // TODO: after https://github.com/elastic/elasticsearch/pull/12006 use ElasicsearchException with addHeader(EXPIRED_FEATURE_HEADER, feature)
        return new ElasticsearchException.WithRestHeadersException("license expired for feature [" + feature + "]",
                Tuple.tuple(EXPIRED_FEATURE_HEADER, new String[] {feature}));
    }
}
