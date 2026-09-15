/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest;

/**
 * Decides whether a REST request may use browser-safelisted content types such as
 * {@code application/x-www-form-urlencoded}.
 */
@FunctionalInterface
public interface RestContentTypePolicy {

    /**
     * Whether this request may use browser-safelisted content types such as
     * {@code application/x-www-form-urlencoded}. Form-encoded POST bodies still
     * require an explicit handler opt-in via {@link RestHandler#supportsReadOnlyFormEncodedPostBody()}.
     */
    boolean allowsBrowserSafelistedContentType(RestRequest request);

    /** Default policy disallows browser-safelisted content types. */
    static RestContentTypePolicy getDefault() {
        return request -> false;
    }
}
