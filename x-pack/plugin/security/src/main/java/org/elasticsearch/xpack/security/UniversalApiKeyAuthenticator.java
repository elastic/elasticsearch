/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.CharArrays;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKey;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authc.Authenticator;

import java.util.Arrays;
import java.util.Base64;

import static org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef.newUniversalApiKeyRealmRef;

public class UniversalApiKeyAuthenticator implements Authenticator {

    private static final Logger logger = LogManager.getLogger(UniversalApiKeyAuthenticator.class);
    private static final String PREFIX = "ApiKey essu_";

    @Override
    public String name() {
        return "Universal API key";
    }

    @Override
    public AuthenticationToken extractCredentials(Context context) {
        SecureString apiKeyString = extractCredentialFromHeaderValue(context.getThreadContext().getHeader("Authorization"));
        if (apiKeyString == null) {
            return null;
        }
        return parseApiKey(apiKeyString);
    }

    static SecureString extractCredentialFromHeaderValue(String header) {
        if (Strings.hasText(header)
            && header.regionMatches(true, 0, UniversalApiKeyAuthenticator.PREFIX, 0, UniversalApiKeyAuthenticator.PREFIX.length())
            && header.length() > UniversalApiKeyAuthenticator.PREFIX.length()) {
            char[] chars = new char[header.length() - UniversalApiKeyAuthenticator.PREFIX.length()];
            header.getChars(UniversalApiKeyAuthenticator.PREFIX.length(), header.length(), chars, 0);
            return new SecureString(chars);
        }
        return null;
    }

    private static ApiKeyService.ApiKeyCredentials parseApiKey(SecureString apiKeyString) {
        if (apiKeyString != null) {
            final byte[] decodedApiKeyCredBytes = Base64.getDecoder().decode(CharArrays.toUtf8Bytes(apiKeyString.getChars()));
            char[] apiKeyCredChars = null;
            try {
                apiKeyCredChars = CharArrays.utf8BytesToChars(decodedApiKeyCredBytes);

                int colonIndex = -1;
                for (int i = 0; i < apiKeyCredChars.length; i++) {
                    if (apiKeyCredChars[i] == ':') {
                        colonIndex = i;
                        break;
                    }
                }

                if (colonIndex < 1) {
                    throw new IllegalArgumentException("invalid ApiKey value");
                }
                final int secretStartPos = colonIndex + 1;
                return new ApiKeyService.ApiKeyCredentials(
                    new String(Arrays.copyOfRange(apiKeyCredChars, 0, colonIndex)),
                    new SecureString(Arrays.copyOfRange(apiKeyCredChars, secretStartPos, apiKeyCredChars.length)),
                    ApiKey.Type.UNIVERSAL
                );
            } finally {
                if (apiKeyCredChars != null) {
                    Arrays.fill(apiKeyCredChars, (char) 0);
                }
            }
        }
        return null;
    }

    @Override
    public void authenticate(Context context, ActionListener<AuthenticationResult<Authentication>> listener) {
        final AuthenticationToken authenticationToken = context.getMostRecentAuthenticationToken();
        if (false == authenticationToken instanceof ApiKeyService.ApiKeyCredentials) {
            listener.onResponse(AuthenticationResult.notHandled());
            return;
        }
        ApiKeyService.ApiKeyCredentials apiKeyCredentials = (ApiKeyService.ApiKeyCredentials) authenticationToken;
        if (apiKeyCredentials.getExpectedType() != ApiKey.Type.UNIVERSAL) {
            listener.onResponse(AuthenticationResult.notHandled());
            return;
        }
        logger.info("Authenticating with Universal API key [{}]", apiKeyCredentials.principal());
        listener.onResponse(AuthenticationResult.success(newAuthentication(apiKeyCredentials.principal())));
    }

    public Authentication newAuthentication(String principal) {
        final Authentication.RealmRef authenticatedBy = newUniversalApiKeyRealmRef("node");
        // roles would get resolved with a call to the IAM service
        return Authentication.newRealmAuthentication(new User(principal, "cloud_user"), authenticatedBy);
    }
}
