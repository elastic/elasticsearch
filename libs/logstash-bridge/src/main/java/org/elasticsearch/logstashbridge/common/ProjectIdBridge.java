/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.logstashbridge.common;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.logstashbridge.StableBridgeAPI;

/**
 * A {@link StableBridgeAPI} for {@link ProjectId}
 */
public interface ProjectIdBridge extends StableBridgeAPI<ProjectId> {
    String id();

    static ProjectIdBridge fromInternal(final ProjectId projectId) {
        return new ProxyInternal(projectId);
    }

    static ProjectIdBridge fromId(final String id) {
        final ProjectId internal = ProjectId.fromId(id);
        return new ProxyInternal(internal);
    }

    static ProjectIdBridge getDefault() {
        return ProxyInternal.DEFAULT;
    }

    /**
     * An implementation of {@link ProjectIdBridge} that proxies calls to
     * an internal {@link ProjectId} instance.
     *
     * @see StableBridgeAPI.ProxyInternal
     */
    final class ProxyInternal extends StableBridgeAPI.ProxyInternal<ProjectId> implements ProjectIdBridge {
        private static final ProjectIdBridge.ProxyInternal DEFAULT = new ProjectIdBridge.ProxyInternal(ProjectId.DEFAULT);

        ProxyInternal(ProjectId internalDelegate) {
            super(internalDelegate);
        }

        @Override
        public String id() {
            return this.internalDelegate.id();
        }

        @Override
        public ProjectId toInternal() {
            return this.internalDelegate;
        }
    }
}
