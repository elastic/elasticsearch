/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.crypto;

import java.util.Collection;
import java.util.Collections;

/**
 * SPI extension point for plugins that contribute {@link EncryptedDataHandler}s to the primary encryption key rotation coordinator.
 *
 * <p>A plugin contributes handlers by declaring {@code extendedPlugins = ['x-pack-security']} in its {@code esplugin {}} block,
 * (x-pack-core is typically already an extended plugin) and shipping a
 * {@code META-INF/services/org.elasticsearch.xpack.core.crypto.EncryptedDataHandlerProvider} entry pointing at the
 * implementation class.
 *
 * <p>Implementations may declare either a no-arg constructor or a one-arg constructor taking the contributing
 * {@code org.elasticsearch.plugins.Plugin} instance. The latter lets implementations capture the parent plugin and resolve services
 * from it lazily at {@code reEncrypt} time, since extensions are constructed before {@code createComponents} runs on the producer.
 */
public interface EncryptedDataHandlerProvider {

    /**
     * Returns the handlers this provider contributes.
     */
    default Collection<EncryptedDataHandler> getHandlers() {
        return Collections.emptyList();
    }
}
