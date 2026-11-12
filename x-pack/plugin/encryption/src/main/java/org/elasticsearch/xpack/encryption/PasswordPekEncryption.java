/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Password-based {@link ProjectEncryptionKeyMetadata.PekEncryption} implementation. Wraps and unwraps PEK bytes for disk (GATEWAY)
 * serialization using PBKDF2-derived AES-256-GCM (see {@link PasswordBasedEncryption}).
 *
 * <p>The active password id and password material are sourced from a {@link Supplier} so that secure-settings reloads are reflected
 * automatically: the supplier is re-evaluated on every call.
 */
final class PasswordPekEncryption implements ProjectEncryptionKeyMetadata.PekEncryption {

    private static final Logger logger = LogManager.getLogger(PasswordPekEncryption.class);

    private final Supplier<Settings> settingsSupplier;
    private final AtomicReference<String> lastWrapFailure = new AtomicReference<>();
    private final ProjectEncryptionKeyMetadata.GatewayKeys gatewayKeys = new ProjectEncryptionKeyMetadata.GatewayKeys();

    PasswordPekEncryption(Supplier<Settings> settingsSupplier) {
        this.settingsSupplier = settingsSupplier;
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

    @Override
    public void onGatewayWrite(@Nullable Exception wrapFailure, boolean keysWritten) {
        String failure = wrapFailure == null ? null : Objects.requireNonNullElse(wrapFailure.getMessage(), wrapFailure.toString());
        String previous = lastWrapFailure.getAndSet(failure);
        if (failure != null && failure.equals(previous) == false) {
            String persisted = keysWritten
                ? "with this node's previous copy of the project encryption key"
                : "without the project encryption key";
            if (ProjectEncryptionKeyPasswordSettings.canWrapForDisk(settingsSupplier.get())) {
                logger.warn(
                    () -> Strings.format("cannot wrap the project encryption key for disk; cluster state was persisted %s", persisted),
                    wrapFailure
                );
            } else {
                logger.warn(
                    "{}; cluster state was persisted {}. Configure [{}] and the matching [{}<id>] secure setting and reload secure"
                        + " settings",
                    failure,
                    persisted,
                    ProjectEncryptionKeyPasswordSettings.ACTIVE_PASSWORD_ID_KEY,
                    ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX
                );
            }
        } else if (failure == null && previous != null) {
            logger.info("project encryption key is persisted to this node's disk again");
        }
    }

    @Override
    public ProjectEncryptionKeyMetadata.GatewayKeys gatewayKeys() {
        return gatewayKeys;
    }

    @Nullable
    String gatewayPasswordId() {
        return gatewayKeys.passwordId();
    }

    /**
     * Returns whether the cluster state this node last read or persisted carried project encryption key material, or {@code null} if
     * this node has done neither, such as a node that is not the elected master in serverless.
     */
    @Nullable
    Boolean isKeyInGateway() {
        return gatewayKeys.holdsKeys();
    }
}
