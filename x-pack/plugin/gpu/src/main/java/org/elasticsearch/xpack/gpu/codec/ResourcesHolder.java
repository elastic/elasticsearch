/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu.codec;

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
