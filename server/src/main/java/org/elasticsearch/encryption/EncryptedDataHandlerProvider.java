/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.encryption;

import java.util.Collection;
import java.util.Collections;

/**
 * SPI extension point for plugins that contribute {@link EncryptedDataHandler}s to the primary encryption key rotation coordinator.
 *
 * <p>Implementations are discovered via {@link org.elasticsearch.plugins.PluginsService#loadServiceProviders} — drop a
 * {@code META-INF/services/org.elasticsearch.encryption.EncryptedDataHandlerProvider} entry in the contributing plugin's resources
 * pointing to the implementation class. The implementation class may declare either a no-arg constructor or a one-arg constructor
 * taking the contributing {@code Plugin} instance (the latter is how implementations get at their own runtime state).
 *
 * <p>Discovered handlers are added to the node-wide {@link EncryptedDataHandlerRegistry} after plugin construction completes.
 */
public interface EncryptedDataHandlerProvider {

    /**
     * Returns the handlers this provider contributes. Called once during node construction.
     */
    default Collection<EncryptedDataHandler> getHandlers() {
        return Collections.emptyList();
    }
}
