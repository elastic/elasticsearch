/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.util.function.BooleanSupplier;

/**
 * Which credential mechanisms a storage provider may be built with on this node, decided by the node's cluster settings
 * and consulted at provider build time, where the typed configuration and therefore the resolved auth mode exist.
 * <p>
 * The same gates run at PUT time in {@link FileDataSourceValidator}, but a configuration can reach a provider build
 * without a PUT: an inline {@code WITH}, or a plan that arrived over the wire. Checking here, after the configuration is
 * parsed and before any client is constructed, means a refused mode never allocates an STS or token-exchange client.
 * The federated check follows the validator's rule, any federated field present refuses, rather than the resolved mode
 * alone, which reads {@code auth=static_credentials} plus a stray {@code role_arn} as static.
 * <p>
 * The check runs when a provider is built, which the registry's per-config pool does once per distinct configuration;
 * turning a mechanism off afterwards does not evict a provider already built with it, the same as the registry's own
 * managed-identity gate. The pool expires an idle client a few minutes after its last use.
 */
public interface CredentialPolicy {

    /** Both mechanisms allowed; for tests and for construction paths that gate elsewhere. */
    CredentialPolicy ALL_ENABLED = of(() -> true, () -> true);

    boolean managedIdentityEnabled();

    boolean federatedIdentityEnabled();

    static CredentialPolicy of(BooleanSupplier managedIdentityEnabled, BooleanSupplier federatedIdentityEnabled) {
        return new CredentialPolicy() {
            @Override
            public boolean managedIdentityEnabled() {
                return managedIdentityEnabled.getAsBoolean();
            }

            @Override
            public boolean federatedIdentityEnabled() {
                return federatedIdentityEnabled.getAsBoolean();
            }
        };
    }

    /**
     * Refuses {@code config} if it selects a mechanism this policy does not allow; a no-op otherwise.
     *
     * @throws IllegalArgumentException with the same message the PUT validator gives for the same configuration
     */
    default void check(FileDataSourceConfiguration config) {
        FileDataSourceConfiguration.AuthMode mode = config.resolveAuthModeOrNull();
        if ((mode == FileDataSourceConfiguration.AuthMode.FEDERATED_IDENTITY || config.hasFederatedAuth())
            && federatedIdentityEnabled() == false) {
            throw new IllegalArgumentException(FileDataSourceValidator.FEDERATED_IDENTITY_DISABLED_MESSAGE);
        }
        if (mode == FileDataSourceConfiguration.AuthMode.MANAGED_IDENTITY && managedIdentityEnabled() == false) {
            throw new IllegalArgumentException(FileDataSourceConfiguration.MANAGED_IDENTITY_DISABLED_MESSAGE);
        }
    }
}
