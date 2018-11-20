/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.proto;

import java.util.Locale;

/**
 * The type of the REST request
 */
public enum RestClient {
    CANVAS,
    CLI,
    REST;
    
    public static RestClient fromString(String restClient) {
        if (restClient == null) {
            return null;
        }
        try {
            return RestClient.valueOf(restClient.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return RestClient.REST;
        }
    }

    @Override
    public String toString() {
        return this.name().toLowerCase(Locale.ROOT);
    }
}
