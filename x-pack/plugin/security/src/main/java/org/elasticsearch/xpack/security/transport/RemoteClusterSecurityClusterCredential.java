/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authc.ApiKeyUtil;

public record RemoteClusterSecurityClusterCredential(ApiKeyService.ApiKeyCredentials fcApiKeyCredentials) {

    public static RemoteClusterSecurityClusterCredential readFromThreadContextHeader(final ThreadContext ctx) {
        final String fcApiKeyBase64 = ctx.getHeader(AuthenticationField.RCS_CLUSTER_CREDENTIAL_HEADER_KEY);
        return (Strings.isEmpty(fcApiKeyBase64)) ? null : new RemoteClusterSecurityClusterCredential(decode(fcApiKeyBase64));
    }

    public static void writeToContext(final ThreadContext threadContext, ApiKeyService.ApiKeyCredentials fcApiKeyCredentials) {
        threadContext.putHeader(AuthenticationField.RCS_CLUSTER_CREDENTIAL_HEADER_KEY, encode(fcApiKeyCredentials));
    }

    private static ApiKeyService.ApiKeyCredentials decode(final String fcApiKey) {
        return decode(new SecureString(fcApiKey.toCharArray()));
    }

    private static ApiKeyService.ApiKeyCredentials decode(final SecureString fcApiKeySecureString) {
        return ApiKeyUtil.toApiKeyCredentials(fcApiKeySecureString);
    }

    private static String encode(final ApiKeyService.ApiKeyCredentials fcApiKey) {
        return fcApiKey.toString();
    }
}
