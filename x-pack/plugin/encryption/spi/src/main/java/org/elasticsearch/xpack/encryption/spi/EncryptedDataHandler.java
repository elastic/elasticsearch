/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption.spi;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.xpack.core.encryption.EncryptedData;

import java.util.function.UnaryOperator;

/**
 * Implemented by features that own a project-scoped {@link Metadata.ProjectCustom} containing data encrypted under the project
 * encryption key (PEK). The handler's single responsibility is knowing <em>where</em> the {@link EncryptedData} values live inside
 * its custom: {@link #reEncrypt} applies a caller-supplied re-keying function to every value. The encryption plugin supplies the
 * function per operation — key rotation, re-wrapping under a snapshot password, or re-wrapping under the destination PEK on
 * restore — so handlers never touch key material, passwords, or plaintext.
 *
 * <p>{@code POST /_encryption/_reset} drives {@link #onDestructiveReset} when the PEK is destroyed.
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
     * Applies {@code reEncrypt} to every {@link EncryptedData} value in {@code current} and returns the rebuilt custom.
     *
     * <p>Contract for applying the function's result:
     * <ul>
     *   <li>same instance — the value is unchanged; keep it.</li>
     *   <li>different {@link EncryptedData} — replace the value.</li>
     *   <li>{@code null} — the value is unrecoverable; clear the field while preserving the rest of the custom.</li>
     * </ul>
     *
     * @param current   the current value of the custom, never {@code null}
     * @param reEncrypt the re-keying function supplied by the encryption plugin
     * @return the rebuilt custom, or the same instance if no value changed. Must not be {@code null}.
     */
    T reEncrypt(T current, UnaryOperator<EncryptedData> reEncrypt);

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
