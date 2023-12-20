/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.blobstore.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;

/**
 * A base abstract blob container that implements higher level container methods.
 */
public abstract class AbstractBlobContainer implements BlobContainer {

    private final BlobPath path;

    protected AbstractBlobContainer(BlobPath path) {
        this.path = path;
    }

    /**
     * Temporary check that permits disabling CAS operations at runtime; TODO remove this when no longer needed
     */
    protected static boolean skipCas(ActionListener<?> listener) {
        if ("true".equals(System.getProperty("test.repository_test_kit.skip_cas"))) {
            listener.onFailure(new UnsupportedOperationException());
            return true;
        }
        return false;
    }

    @Override
    public BlobPath path() {
        return this.path;
    }

    @Override
    public String toString() {
        return getClass() + "{" + path + "}";
    }
}
