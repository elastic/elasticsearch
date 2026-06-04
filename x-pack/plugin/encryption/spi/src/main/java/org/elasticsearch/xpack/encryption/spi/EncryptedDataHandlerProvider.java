/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption.spi;

import java.util.Collection;
import java.util.Collections;

/**
 * SPI extension point for plugins that contribute {@link EncryptedDataHandler}s to the project encryption key rotation coordinator.
 *
 * <p>A plugin contributes handlers by declaring {@code extendedPlugins = ['x-pack-encryption']} in its {@code esplugin {}} block,
 * adding a {@code compileOnly project(':x-pack:plugin:encryption:spi')} dependency, and shipping a
 * {@code META-INF/services/org.elasticsearch.xpack.encryption.spi.EncryptedDataHandlerProvider} entry pointing at the
 * implementation class.
 */
public interface EncryptedDataHandlerProvider {

    /**
     * Returns the handlers this provider contributes.
     */
    default Collection<EncryptedDataHandler<?>> getHandlers() {
        return Collections.emptyList();
    }

    /**
     * Called by the encryption plugin after the {@link EncryptionService} has been created, before any plugin's
     * {@code createComponents} is invoked. Implementations may store the service for use during component creation.
     *
     * <p>The default implementation is a no-op.
     */
    default void onEncryptionServiceAvailable(EncryptionService encryptionService) {}
}
