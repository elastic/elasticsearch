/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.cluster.metadata.ProjectId;

import java.util.Collection;

/**
 * Service for registering {@link LinkedProjectConfigListener}s to be notified of changes to linked project configurations.
 */
public interface LinkedProjectConfigService {

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

        /**
         * Called when the boolean skip_unavailable setting has changed for a linked project configuration.
         * Note that skip_unavailable may not be supported in all contexts where linked projects are used.
         *
         * @param originProjectId The {@link ProjectId} of the owning project that has the linked project configuration.
         * @param linkedProjectId The {@link ProjectId} of the linked project.
         * @param linkedProjectAlias The alias used for the linked project.
         * @param skipUnavailable The new value of the skip_unavailable setting.
         */
        default void skipUnavailableChanged(
            ProjectId originProjectId,
            ProjectId linkedProjectId,
            String linkedProjectAlias,
            boolean skipUnavailable
        ) {}
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
