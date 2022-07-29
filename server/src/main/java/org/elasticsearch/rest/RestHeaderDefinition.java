/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest;

/**
 * A definition for an http header that should be copied to the {@link org.elasticsearch.common.util.concurrent.ThreadContext} when
 * reading the request on the rest layer.
 */
public final class RestHeaderDefinition {
    private final String name;
    /**
     * This should be set to true only when the syntax of the value of the Header to copy is defined as a comma separated list of String
     * values.
     */
    private final boolean multiValueAllowed;

    public RestHeaderDefinition(String name, boolean multiValueAllowed) {
        this.name = name;
        this.multiValueAllowed = multiValueAllowed;
    }

    public String getName() {
        return name;
    }

    public boolean isMultiValueAllowed() {
        return multiValueAllowed;
    }
}
