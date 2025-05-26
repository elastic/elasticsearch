/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.CloudApiKeyService;

public class CloudApiKeyAuthenticator implements Authenticator {
    private final String nodeName;
    private final CloudApiKeyService cloudApiKeyService;

    public CloudApiKeyAuthenticator(String nodeName, CloudApiKeyService cloudApiKeyService) {
        this.nodeName = nodeName;
        this.cloudApiKeyService = cloudApiKeyService;
    }

    @Override
    public String name() {
        return "cloud api key";
    }

    @Override
    public AuthenticationToken extractCredentials(Context context) {
        return cloudApiKeyService.extractCloudApiKey(context.getThreadContext());
    }

    @Override
    public void authenticate(Context context, ActionListener<AuthenticationResult<Authentication>> listener) {
        final AuthenticationToken authenticationToken = context.getMostRecentAuthenticationToken();
        if (false == authenticationToken instanceof CloudApiKeyService.CloudApiKey) {
            listener.onResponse(AuthenticationResult.notHandled());
            return;
        }

        final CloudApiKeyService.CloudApiKey cloudApiKey = (CloudApiKeyService.CloudApiKey) authenticationToken;

        cloudApiKeyService.authenticate(cloudApiKey, listener.delegateFailureAndWrap((l, authenticationResult) -> {
            if (authenticationResult.isAuthenticated()) {
                l.onResponse(AuthenticationResult.success(Authentication.newCloudApiKeyAuthentication(authenticationResult, nodeName)));
                // TODO handle termination etc.
            } else {
                l.onResponse(AuthenticationResult.notHandled());
            }
        }));
    }
}
