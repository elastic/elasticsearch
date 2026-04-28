/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.cloud;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.authc.Authentication;

/**
 * SPI for managing UIAM cloud credentials, to enable cross-project search (CPS) support for ML.
 * <p>
 * At transform creation, the authenticating user's cloud credential is extracted from the
 * {@link ThreadContext} and serialized as a base64-encoded string. At execution time, the
 * persisted credential is decoded and injected back into the {@link ThreadContext} so
 * that the CPS transport interceptor can use it to authenticate cross-project requests.
 * <p>
 * The serverless security plugin provides a real implementation, which is loaded/bound by the Security plugin.
 * In a non-serverless environment, the {@link Default} no-op implementation is used instead.
 */
public interface InternalCloudApiKeyService {
    record CloudGrantApiKeyResult(CloudCredential credential, Authentication authentication) {}

    @Nullable
    CloudCredential extractCloudManagedCredential(ThreadContext threadContext);

    void injectCloudManagedCredential(ThreadContext context, CloudCredential storedCredential);

    /**
     * Grants a dedicated UIAM Cloud API key for an ML job, using the caller's credential to authenticate
     * the grant request. The granted key is then authenticated to produce an {@link Authentication}
     * that the caller can persist alongside the credential.
     *
     * @param cloudManagedCredential the base64-encoded caller credential (from {@link #extractCloudManagedCredential})
     * @param description      a human-readable description for the granted key (e.g. transform ID)
     * @param listener         receives the granted credential and its corresponding {@link Authentication}
     */
    void grantCloudAuthentication(
        ThreadContext threadContext,
        CloudCredential cloudManagedCredential,
        String description,
        ActionListener<CloudGrantApiKeyResult> listener
    );

    interface Provider {
        InternalCloudApiKeyService getInternalCloudApiKeyService();

        class Default implements Provider {
            @Override
            public InternalCloudApiKeyService getInternalCloudApiKeyService() {
                return new InternalCloudApiKeyService.Default();
            }
        }
    }

    class Default implements InternalCloudApiKeyService {
        @Override
        public CloudCredential extractCloudManagedCredential(ThreadContext threadContext) {
            return null;
        }

        @Override
        public void injectCloudManagedCredential(ThreadContext context, CloudCredential storedCredential) {

        }

        @Override
        public void grantCloudAuthentication(
            ThreadContext threadContext,
            CloudCredential cloudManagedCredential,
            String description,
            ActionListener<CloudGrantApiKeyResult> listener
        ) {
            listener.onFailure(new UnsupportedOperationException("UIAM cloud authentication is not available outside serverless"));
        }
    }
}
