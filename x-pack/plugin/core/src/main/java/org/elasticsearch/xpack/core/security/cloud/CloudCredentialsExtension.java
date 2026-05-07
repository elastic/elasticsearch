/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.cloud;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Extension point for handling cloud credentials, including service for granting
 * and revoking API keys as well as manager for managing the cloud credentials at runtime.
 */
public interface CloudCredentialsExtension {

    InternalCloudApiKeyService internalCloudApiKeyService();

    CloudCredentialManager credentialManager();

    // TODO this is a hack to unblock the ML CPS integration; ideally we'd use SPI for this,
    // but we need to solve:
    // 1) An implementation (i.e. `Default`) needs to be available when Security is disabled
    // 2) How to wire the serverless implementation into places that can't use `@Inject` (e.g. `Transform`)
    AtomicReference<CloudCredentialsExtension> REFERENCE = new AtomicReference<>(new Default());

    static CloudCredentialsExtension getInstance() {
        return REFERENCE.get();
    }

    static void setInstance(CloudCredentialsExtension instance) {
        REFERENCE.set(instance);
    }

    /** No-op default used when serverless security is not loaded. */
    class Default implements CloudCredentialsExtension {

        private static final InternalCloudApiKeyService API_KEY_SERVICE = new InternalCloudApiKeyService.Default();
        private static final CloudCredentialManager CREDENTIAL_MANAGER = new CloudCredentialManager.Default();

        @Override
        public InternalCloudApiKeyService internalCloudApiKeyService() {
            return API_KEY_SERVICE;
        }

        @Override
        public CloudCredentialManager credentialManager() {
            return CREDENTIAL_MANAGER;
        }
    }
}
