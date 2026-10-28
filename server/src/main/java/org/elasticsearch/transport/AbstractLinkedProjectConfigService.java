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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Abstract base class for {@link LinkedProjectConfigService} implementations.
 * Provides common functionality for managing a list of registered listeners and notifying them of updates.
 */
public abstract class AbstractLinkedProjectConfigService implements LinkedProjectConfigService {
    private final List<LinkedProjectConfigListener> listeners = new CopyOnWriteArrayList<>();

    @Override
    public void register(LinkedProjectConfigListener listener) {
        listeners.add(listener);
    }

    protected void handleUpdate(LinkedProjectConfig config) {
        listeners.forEach(listener -> listener.updateLinkedProject(config));
    }

    protected void handleRemoved(ProjectId originProjectId, ProjectId linkedProjectId, String linkedProjectAlias) {
        listeners.forEach(listener -> listener.remove(originProjectId, linkedProjectId, linkedProjectAlias));
    }
}
