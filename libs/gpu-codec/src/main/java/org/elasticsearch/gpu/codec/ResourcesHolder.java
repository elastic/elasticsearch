/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gpu.codec;

/**
 * Holds an acquired resource, allows to manually release it, but still ensures it gets released (closed)
 * at the end of a try-with-resources block via the {@link AutoCloseable} pattern.
 */
class ResourcesHolder implements AutoCloseable {

    private final CuVSResourceManager resourceManager;
    private CuVSResourceManager.ManagedCuVSResources managedResource;

    ResourcesHolder(CuVSResourceManager cuVSResourceManager, CuVSResourceManager.ManagedCuVSResources managedResource) {
        this.resourceManager = cuVSResourceManager;
        this.managedResource = managedResource;
    }

    CuVSResourceManager.ManagedCuVSResources resources() {
        return managedResource;
    }

    void release() {
        if (managedResource != null) {
            var toRelease = managedResource;
            managedResource = null;
            resourceManager.release(toRelease);
        }
    }

    @Override
    public void close() {
        release();
    }
}
