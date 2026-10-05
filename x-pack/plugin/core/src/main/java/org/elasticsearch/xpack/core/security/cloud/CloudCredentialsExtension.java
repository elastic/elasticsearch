/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.cloud;

import org.elasticsearch.common.util.FeatureFlag;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Extension point bundling the cloud credential services: {@link InternalCloudApiKeyService} and
 * {@link CloudCredentialManager}.
 */
public interface CloudCredentialsExtension {

    FeatureFlag ML_CROSS_PROJECT = new FeatureFlag("ml_cross_project");

    InternalCloudApiKeyService internalCloudApiKeyService();

    CloudCredentialManager credentialManager();

    // TODO this is a hack to unblock the ML CPS integration; ideally we'd use SPI for this,
    // but we need to solve:
    // 1) An implementation (i.e. `Noop`) needs to be available when Security is disabled
    // 2) How to wire the serverless implementation into places that can't use `@Inject` (e.g. `Transform`)
    AtomicReference<CloudCredentialsExtension> REFERENCE = new AtomicReference<>(new Noop());

    static CloudCredentialsExtension getInstance() {
        return REFERENCE.get();
    }

    /**
     * Sets the active extension.
     */
    static void setInstance(CloudCredentialsExtension instance) {
        Objects.requireNonNull(instance, "extension instance must not be null");
        REFERENCE.set(instance);
    }

    /**
     * No-op default used when no real implementation is loaded.
     */
    class Noop implements CloudCredentialsExtension {

        private static final InternalCloudApiKeyService API_KEY_SERVICE = new InternalCloudApiKeyService.Default();
        private static final CloudCredentialManager CREDENTIAL_MANAGER = new CloudCredentialManager.Noop();

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
