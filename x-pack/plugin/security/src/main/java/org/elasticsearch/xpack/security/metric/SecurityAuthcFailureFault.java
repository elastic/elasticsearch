/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.metric;

/**
 * Rough bucket for who probably "owns" a failed auth when we record metrics: client vs server vs unknown.
 * Values are fixed strings on purpose so the label stays low-cardinality.
 * CLIENT is the usual invalid credentials / 4xx-style case, SERVER is for 5xx, I/O, timeouts,
 * UNKNOWN is everything else we did not map.
 */
public enum SecurityAuthcFailureFault {

    CLIENT("client"),
    SERVER("server"),
    UNKNOWN("unknown");

    public static final String ATTRIBUTE_FAILURE_FAULT = "es_security_authc_failure_fault";

    private final String attributeValue;

    SecurityAuthcFailureFault(String attributeValue) {
        this.attributeValue = attributeValue;
    }

    public String attributeValue() {
        return attributeValue;
    }
}
