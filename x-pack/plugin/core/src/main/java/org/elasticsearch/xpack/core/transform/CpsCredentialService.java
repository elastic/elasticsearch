/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;

import java.util.concurrent.atomic.AtomicReference;

/**
 * SPI for managing cross-project search (CPS) credentials in the transform lifecycle.
 * <p>
 * At transform creation, the authenticating user's cloud credential is extracted from the
 * {@link ThreadContext} and serialized as a base64-encoded string. At execution time, the
 * persisted credential is decoded and injected back into the {@link ThreadContext} so
 * that the CPS transport interceptor can use it to authenticate cross-project requests.
 * <p>
 * The serverless security plugin registers the real implementation via {@link #setInstance};
 * consumers resolve it via {@link #getInstance}, which falls back to {@link #NOOP} when
 * running outside serverless.
 */
public interface CpsCredentialService {

    CpsCredentialService NOOP = new CpsCredentialService() {
        @Override
        public String extractCpsCredential(ThreadContext threadContext) {
            return null;
        }

        @Override
        public void injectCpsCredential(ThreadContext threadContext, String storedCredential) {}

        @Override
        public void grantCpsCredential(String callerCredential, String description, ActionListener<String> listener) {
            listener.onResponse(callerCredential);
        }
    };

    AtomicReference<CpsCredentialService> REGISTERED = new AtomicReference<>(NOOP);

    static void setInstance(CpsCredentialService service) {
        REGISTERED.set(service);
    }

    static CpsCredentialService getInstance() {
        return REGISTERED.get();
    }

    @Nullable
    String extractCpsCredential(ThreadContext threadContext);

    void injectCpsCredential(ThreadContext threadContext, String storedCredential);

    /**
     * Grants a dedicated CPS API key for a transform, using the caller's credential to authenticate
     * the grant request. The resulting credential replaces the caller's credential for persistence.
     *
     * @param callerCredential the base64-encoded caller credential (from {@link #extractCpsCredential})
     * @param description      a human-readable description for the granted key (e.g. transform ID)
     * @param listener         receives the base64-encoded granted credential to persist
     */
    void grantCpsCredential(String callerCredential, String description, ActionListener<String> listener);
}
