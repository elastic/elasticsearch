/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.retry;

import org.apache.http.HttpResponse;

public class ResponseHandlerUtils {
    public static String getFirstHeaderOrUnknown(HttpResponse response, String name) {
        var header = response.getFirstHeader(name);
        if (header != null && header.getElements().length > 0) {
            return header.getElements()[0].getName();
        }
        return "unknown";
    }

    private ResponseHandlerUtils() {}
}
