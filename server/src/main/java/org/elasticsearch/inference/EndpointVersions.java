/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

public enum EndpointVersions {
    FIRST_ENDPOINT_VERSION("2023-09-29"),
    PARAMETERS_INTRODUCED_ENDPOINT_VERSION("2024-10-17");

    private final String name;

    EndpointVersions(String s) {
        name = s;
    }

    public String toString() {
        return this.name;
    }
}
