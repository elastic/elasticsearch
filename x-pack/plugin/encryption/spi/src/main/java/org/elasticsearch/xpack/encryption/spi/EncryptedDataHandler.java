/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption.spi;

import org.elasticsearch.cluster.metadata.Metadata;

import java.util.function.UnaryOperator;

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
     * Returns a copy of {@code current} where each {@link EncryptedData} value has been passed through {@code rewrapper}. The caller
     * builds the rewrapper to express the desired transformation — key rotation, snapshot re-wrapping, or drop (when {@code rewrapper}
     * returns {@code null} for a value). If all values are dropped the implementation should return {@code null} to signal removal.
     *
     * @param current   the current value of the custom in cluster state, or {@code null} if absent
     * @param rewrapper function that transforms each {@link EncryptedData} — returns {@code null} to drop a value, or the original
     *                  instance (same reference) to signal no change for that value
     * @return the transformed custom (may be same instance if no values changed), or {@code null} to remove it from state
     */
    T reEncrypt(T current, UnaryOperator<EncryptedData> rewrapper);

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
