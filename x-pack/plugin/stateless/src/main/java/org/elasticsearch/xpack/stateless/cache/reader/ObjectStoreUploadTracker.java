/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache.reader;

import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

/**
 * Used to determine whether a batched compound commit has been uploaded to the object store.
 */
public interface ObjectStoreUploadTracker {

    interface UploadInfo {

        boolean isUploaded();

        String preferredNodeId();
    }

    UploadInfo getLatestUploadInfo(PrimaryTermAndGeneration bccTermAndGen);
}
