/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.ClusterSecrets;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;

import java.util.Set;
import java.util.function.Supplier;

/**
 * Password-based {@link ProjectEncryptionKeyMetadata.PekEncryption} implementation. Wraps and unwraps PEK bytes for disk (GATEWAY)
 * serialization using PBKDF2-derived AES-256-GCM (see {@link PasswordBasedEncryption}).
 *
 * <p>The active password id and password material are sourced from a {@link Supplier} so that secure-settings reloads are reflected
 * automatically: the supplier is re-evaluated on every call.
 */
final class PasswordPekEncryption implements ProjectEncryptionKeyMetadata.PekEncryption {

    private final Supplier<Settings> settingsSupplier;
    private final ProjectEncryptionKeyMetadata.GatewayKeys gatewayKeys = new ProjectEncryptionKeyMetadata.GatewayKeys();

    private Settings checkedSettings;
    private String wrapFailure;

    PasswordPekEncryption(Supplier<Settings> settingsSupplier) {
        this.settingsSupplier = settingsSupplier;
    }

    @Override
    public ProjectEncryptionKeyMetadata.GatewayKeys gatewayKeys() {
        return gatewayKeys;
    }

    /**
     * Returns whether a node with {@code roles} persists cluster state, and so must be able to wrap the key. Mirrors
     * {@link org.elasticsearch.gateway.GatewayMetaState}, which gives master-eligible and data nodes an on-disk persisted state. On
     * stateless only the elected master persists it, to the object store, so every master-eligible node must be able to.
     */
    static boolean persistsClusterState(Set<DiscoveryNodeRole> roles, boolean stateless) {
        return roles.contains(DiscoveryNodeRole.MASTER_ROLE)
            || (stateless == false && roles.stream().anyMatch(DiscoveryNodeRole::canContainData));
    }

    /**
     * Returns whether nodes needn't be checked for the password, because on stateless it comes from {@link ClusterSecrets} in cluster
     * state, the same on every node. Stateful nodes are always checked: a node persists a cluster state before applying it, so it
     * writes the key with the password it had before the cluster secrets in that state reach it.
     */
    static boolean passwordsFromClusterSecrets(ClusterState state, boolean stateless) {
        ClusterSecrets secrets = state.custom(ClusterSecrets.TYPE);
        return stateless
            && secrets != null
            && secrets.getSettings().getSettingNames().contains(ProjectEncryptionKeyPasswordSettings.ACTIVE_PASSWORD_ID_KEY);
    }

    /** Returns whether this node can wrap the key for its persisted cluster state. */
    boolean canWrapForGateway() {
        return wrapForGatewayFailure() == null;
    }

    /**
     * Returns why this node cannot wrap the key for its persisted cluster state, or {@code null} if it can. Computed once per secure
     * settings snapshot, since it derives a key.
     */
    @Nullable
    synchronized String wrapForGatewayFailure() {
        Settings settings = settingsSupplier.get();
        if (settings != checkedSettings) {
            wrapFailure = ProjectEncryptionKeyPasswordSettings.wrapForGatewayFailure(settings);
            checkedSettings = settings;
        }
        return wrapFailure;
    }

    /** Returns whether this node can decrypt its {@link ProjectEncryptionKeyMetadata.GatewayKeys}, or {@code null} if it holds none. */
    @Nullable
    Boolean canReadGatewayKeys() {
        return gatewayKeys.canRead(this, settingsSupplier.get());
    }

    @Override
    public String activePasswordId() {
        String id = ProjectEncryptionKeyPasswordSettings.getActivePasswordId(settingsSupplier.get());
        if (id == null) {
            throw new ElasticsearchException(
                "cannot wrap PEK for disk: [" + ProjectEncryptionKeyPasswordSettings.ACTIVE_PASSWORD_ID_KEY + "] is not configured"
            );
        }
        return id;
    }

    @Override
    public WrappedKey wrap(byte[] plaintextPek) {
        Settings s = settingsSupplier.get();
        String passwordId = ProjectEncryptionKeyPasswordSettings.getActivePasswordId(s);
        if (passwordId == null) {
            throw new ElasticsearchException(
                "cannot wrap PEK for disk: [" + ProjectEncryptionKeyPasswordSettings.ACTIVE_PASSWORD_ID_KEY + "] is not configured"
            );
        }
        try (SecureString password = ProjectEncryptionKeyPasswordSettings.getPassword(s, passwordId)) {
            if (password == null) {
                throw new ElasticsearchException(
                    "cannot wrap PEK for disk: secure setting ["
                        + ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX
                        + passwordId
                        + "] is not configured"
                );
            }
            return new WrappedKey(passwordId, PasswordBasedEncryption.wrap(plaintextPek, passwordId, password.getChars()).payload());
        }
    }

    @Override
    public byte[] unwrap(byte[] wrappedPek, String passwordId) {
        try (SecureString password = ProjectEncryptionKeyPasswordSettings.getPassword(settingsSupplier.get(), passwordId)) {
            if (password == null) {
                throw new ElasticsearchException(
                    "cannot unwrap PEK from disk: secure setting ["
                        + ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX
                        + passwordId
                        + "] is not configured"
                );
            }
            return PasswordBasedEncryption.unwrap(new EncryptedData(passwordId, wrappedPek), password.getChars());
        }
    }
}
