/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.cloud;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.core.security.authc.Authentication;

/**
 * Service for programmatically granting cloud API keys for ML cross-project search jobs.
 */
public interface InternalCloudApiKeyService {

    record CloudGrantApiKeyResult(PersistedCloudCredential persistedCredential, Authentication authentication) {}

    /**
     * Grants a dedicated cloud API key for an ML job, using the caller's credential to authenticate
     * the grant request. The granted key is then authenticated to produce an {@link Authentication}
     * that the caller can persist alongside the credential.
     *
     * @param cloudManagedCredential the caller credential (extracted from the active request via
     *                               {@link CloudCredentialManager#extractCloudManagedCredential})
     * @param description            a human-readable description for the granted key, by convention
     *                               {@code "<resource-kind>:<resource-id>"} (for example
     *                               {@code "transform:my-transform"} or {@code "datafeed:my-feed"})
     * @param listener               receives the granted credential envelope and its corresponding
     *                               {@link Authentication}
     */
    void grantCloudAuthentication(
        CloudCredential cloudManagedCredential,
        String description,
        ActionListener<CloudGrantApiKeyResult> listener
    );

    /**
     * Default used when no real implementation is loaded.
     */
    class Default implements InternalCloudApiKeyService {
        @Override
        public void grantCloudAuthentication(
            CloudCredential cloudManagedCredential,
            String description,
            ActionListener<CloudGrantApiKeyResult> listener
        ) {
            listener.onFailure(new UnsupportedOperationException("cloud API key granting is not available outside serverless"));
        }
    }
}
