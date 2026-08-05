/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption.spi;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.core.Nullable;

/**
 * Implemented by features that own a project-scoped {@link Metadata.ProjectCustom} containing data encrypted under the project
 * encryption key (PEK). The rotation coordinator drives {@link #reEncrypt} when the custom is not yet on the active key, and
 * {@code POST /_encryption/_reset} drives {@link #onDestructiveReset} when the PEK is destroyed.
 *
 * <p>Handlers are contributed via the {@link EncryptedDataHandlerProvider} SPI.
 *
 * @param <T> the project-scoped {@link Metadata.ProjectCustom} subtype this handler owns
 */
public interface EncryptedDataHandler<T extends Metadata.ProjectCustom> {

    /**
     * The {@link Metadata.ProjectCustom} name owned by this handler. Must equal {@code T#getWriteableName()}
     */
    String customName();

    /**
     * Returns a re-encrypted copy of {@code current} where every encrypted value is under {@code activeKeyId}.
     *
     * @param current          the current value of the custom in cluster state, or {@code null} if absent
     * @param encryptionService used to decrypt with the previous key and encrypt under the active key
     * @param activeKeyId      the key ID every encrypted value in the returned custom must be under
     * @return the re-encrypted custom. This may be the same instance if no change is needed. This must not be null when {@code current}
     *         is not null.
     */
    T reEncrypt(T current, EncryptionService encryptionService, String activeKeyId);

    /**
     * Serializes {@code current} to a plaintext byte array suitable for inclusion in an encrypted snapshot blob.
     * Only secrets (values encrypted under the PEK) need to be decrypted before serialization; non-secret values
     * may be written as-is.
     *
     * @param current          the current value of the custom, never {@code null} when called
     * @param encryptionService used to decrypt PEK-encrypted secrets before writing them out
     * @return the serialized plaintext bytes, or {@code null} if this handler does not support snapshot export
     */
    @Nullable
    default byte[] toSnapshotBytes(T current, EncryptionService encryptionService) {
        return null;
    }

    /**
     * Deserializes bytes previously produced by {@link #toSnapshotBytes} and re-encrypts any secrets under the
     * destination cluster's PEK.
     *
     * @param bytes            plaintext bytes as produced by {@link #toSnapshotBytes}
     * @param encryptionService used to re-encrypt secrets under the destination PEK
     * @return the restored custom, or {@code null} if this handler does not support snapshot import
     */
    @Nullable
    default T fromSnapshotBytes(byte[] bytes, EncryptionService encryptionService) {
        return null;
    }

    /**
     * Decides what happens to this handler's custom when the project encryption key is destructively reset (via
     * {@code POST /_encryption/_reset}). After reset, every entry encrypted under the previous PEK is unrecoverable.
     *
     * <p>The return value, applied atomically alongside the PEK removal:
     * <ul>
     *   <li>{@code null} — remove the custom from project metadata. Default behavior; appropriate when every value
     *       in the custom is encrypted under the now-destroyed PEK.</li>
     *   <li>same instance as {@code current} — no change.</li>
     *   <li>different non-null {@code T} — replace the custom with the returned value, e.g. when only part of the
     *       custom was encrypted and the unencrypted portion is worth keeping.</li>
     * </ul>
     *
     * @param current the current value of the custom in cluster state, or {@code null} if absent
     * @return the replacement custom, or {@code null} to remove it
     */
    default T onDestructiveReset(T current) {
        return null;
    }
}
