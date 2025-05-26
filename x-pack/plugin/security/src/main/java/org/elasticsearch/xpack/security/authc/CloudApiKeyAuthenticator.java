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
import org.elasticsearch.xpack.core.security.authc.cloud.CloudApiKey;
import org.elasticsearch.xpack.core.security.authc.cloud.CloudApiKeyService;

public class CloudApiKeyAuthenticator implements Authenticator {
    private final String nodeName;
    private final CloudApiKeyService cloudApiKeyService;

    public CloudApiKeyAuthenticator(String nodeName, CloudApiKeyService cloudApiKeyService) {
        this.nodeName = nodeName;
        this.cloudApiKeyService = cloudApiKeyService;
    }

    @Override
    public String name() {
        return "cloud API key";
    }

    @Override
    public AuthenticationToken extractCredentials(Context context) {
        return cloudApiKeyService.parseAsCloudApiKey(context.getApiKeyString());
    }

    @Override
    public void authenticate(Context context, ActionListener<AuthenticationResult<Authentication>> listener) {
        final AuthenticationToken authenticationToken = context.getMostRecentAuthenticationToken();
        if (false == authenticationToken instanceof CloudApiKey) {
            listener.onResponse(AuthenticationResult.notHandled());
            return;
        }

        final CloudApiKey cloudApiKey = (CloudApiKey) authenticationToken;

        cloudApiKeyService.authenticate(
            cloudApiKey,
            nodeName,
            ActionListener.wrap(
                authentication -> listener.onResponse(AuthenticationResult.success(authentication)),
                e -> listener.onFailure(context.getRequest().exceptionProcessingRequest(e, cloudApiKey))
            )
        );
    }
}
