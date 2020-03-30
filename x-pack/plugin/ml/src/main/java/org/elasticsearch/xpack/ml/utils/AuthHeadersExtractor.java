/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.utils;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.support.SecondaryAuthentication;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public final class AuthHeadersExtractor {

    private AuthHeadersExtractor() {}

    public static Map<String, String> extractAuthHeadersAndPreferSecondaryAuth(ThreadContext context) {
        Map<String, String> threadHeaders = context.getHeaders();
        if (threadHeaders == null || threadHeaders.isEmpty()) {
            return new HashMap<>();
        }

        Map<String, String> headers = threadHeaders.entrySet().stream()
            .filter(e -> ClientHelper.SECURITY_HEADER_FILTERS.contains(e.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        String secondaryAuth = headers.remove(SecondaryAuthentication.THREAD_CTX_KEY);
        if (secondaryAuth != null) {
            headers.put(AuthenticationField.AUTHENTICATION_KEY, secondaryAuth);
        }
        return headers;
    }

}
