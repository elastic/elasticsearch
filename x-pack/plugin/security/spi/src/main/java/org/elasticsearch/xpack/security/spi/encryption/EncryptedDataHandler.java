/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.spi.encryption;

import org.elasticsearch.cluster.metadata.Metadata;

/**
 * Implemented by features that own a project-scoped {@link Metadata.ProjectCustom} containing data encrypted under the primary
 * encryption key (PEK). The rotation coordinator drives each handler when its data is not yet on the active key.
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
     * @return the re-encrypted custom
     */
    T reEncrypt(T current, EncryptionService encryptionService, String activeKeyId);
}
