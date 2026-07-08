/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud;

import org.elasticsearch.common.ValidationException;

import java.net.URI;
import java.util.Locale;

/**
 * Validates that a user-supplied endpoint URI for the TencentCloud AI Gateway inference service points to an
 * approved Tencent-hosted host. This is the SSRF guard: because the framework only performs URI syntax parsing
 * (see {@code ServiceUtils.convertToUri}) and the shared HTTP client accepts arbitrary hosts, each service that
 * exposes a user-overridable URL must enforce its own allow-list here, before any secret is bound to a request.
 */
public final class TencentCloudEndpointUtils {

    private static final String TENCENT_ES_SUFFIX = ".tencentelasticsearch.com";
    private static final String TENCENT_ES_ROOT = "tencentelasticsearch.com";

    private TencentCloudEndpointUtils() {}

    public static URI validateEndpoint(URI uri, String fieldName, String scope, ValidationException validationException) {
        if (uri == null) {
            return null;
        }
        String scheme = uri.getScheme();
        if ("https".equalsIgnoreCase(scheme) == false) {
            validationException.addValidationError(
                String.format(Locale.ROOT, "[%s] in [%s] must use the [https] scheme", fieldName, scope)
            );
            return uri;
        }
        String host = uri.getHost();
        if (host == null || host.isBlank()) {
            validationException.addValidationError(String.format(Locale.ROOT, "[%s] in [%s] must include a host", fieldName, scope));
            return uri;
        }
        String normalizedHost = normalizeHost(host);
        // SSRF protection relies on the allow-list below: the host must be a *.tencentelasticsearch.com host.
        // Any IP literal (private or public), localhost, or other non-Tencent host therefore fails this check,
        // so there is no need for a separate blocked-host list.
        if (isTencentElasticsearchHost(normalizedHost) == false) {
            validationException.addValidationError(
                String.format(
                    Locale.ROOT,
                    "[%s] in [%s] must point to a TencentCloud AI Gateway host ending with [%s]",
                    fieldName,
                    scope,
                    TENCENT_ES_SUFFIX
                )
            );
        }
        return uri;
    }

    private static String normalizeHost(String host) {
        String normalized = host.toLowerCase(Locale.ROOT);
        if (normalized.endsWith(".")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }

    private static boolean isTencentElasticsearchHost(String host) {
        return host.equals(TENCENT_ES_ROOT) || host.endsWith(TENCENT_ES_SUFFIX);
    }
}
