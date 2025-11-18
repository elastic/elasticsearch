/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import java.util.Collection;
import java.util.Optional;

/**
 * Service for registering {@link LinkedProjectConfigListener}s to be notified of changes to linked project configurations.
 */
public interface LinkedProjectConfigService {

    /**
     * Interface for providing a {@link LinkedProjectConfigService} instance via SPI.
     */
    interface Provider {
        /**
         * @return An {@link Optional} populated with a {@link LinkedProjectConfigService} instance, or {@link Optional#empty()} if it is
         * not possible to create an instance in the current runtime environment.
         */
        Optional<LinkedProjectConfigService> create();
    }

    /**
     * Listener interface for receiving updates about linked project configurations.
     * Implementations must not throw from any of the interface methods.
     */
    interface LinkedProjectConfigListener {
        /**
         * Called when a linked project configuration has been added or updated.
         *
         * @param config The updated {@link LinkedProjectConfig}.
         */
        void updateLinkedProject(LinkedProjectConfig config);
    }

    /**
     * Registers a {@link LinkedProjectConfigListener} to receive updates about linked project configurations.
     *
     * @param listener The listener to register.
     */
    void register(LinkedProjectConfigListener listener);

    /**
     * Loads all linked project configurations known at node startup, for all origin projects.
     *
     * @return A collection of all known {@link LinkedProjectConfig}s at node startup.
     */
    Collection<LinkedProjectConfig> getInitialLinkedProjectConfigs();
}
