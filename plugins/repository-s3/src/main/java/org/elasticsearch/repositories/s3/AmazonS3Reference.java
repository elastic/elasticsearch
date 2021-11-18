/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Releasable;

/**
 * Handles the shutdown of the wrapped {@link AmazonS3Client} using reference
 * counting.
 */
public class AmazonS3Reference extends AbstractRefCounted implements Releasable {

    private final AmazonS3 client;

    AmazonS3Reference(AmazonS3 client) {
        this.client = client;
    }

    /**
     * Call when the client is not needed anymore.
     */
    @Override
    public void close() {
        decRef();
    }

    /**
     * Returns the underlying `AmazonS3` client. All method calls are permitted BUT
     * NOT shutdown. Shutdown is called when reference count reaches 0.
     */
    public AmazonS3 client() {
        return client;
    }

    @Override
    protected void closeInternal() {
        client.shutdown();
    }

}
