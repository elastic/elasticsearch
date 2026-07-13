/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.instrumentation;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestRequest;

record RequestAndRoute(RestRequest request, String matchedRoute, String urlPath, @Nullable String urlQuery) {

    RequestAndRoute(RestRequest request, String matchedRoute) {
        this(request, matchedRoute, extractPath(request.uri()), extractQuery(request.uri()));
    }

    private static String extractPath(String uri) {
        int i = uri.indexOf('?');
        return i >= 0 ? uri.substring(0, i) : uri;
    }

    private static String extractQuery(String uri) {
        int i = uri.indexOf('?');
        return i >= 0 ? uri.substring(i + 1) : null;
    }
}
