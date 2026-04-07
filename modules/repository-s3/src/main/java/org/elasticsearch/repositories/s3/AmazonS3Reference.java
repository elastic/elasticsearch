/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.services.s3.S3Client;

import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Releasable;

/**
 * Handles the shutdown of the wrapped {@link S3Client} using reference counting.
 */
public class AmazonS3Reference extends AbstractRefCounted implements Releasable {

    private final S3Client client;
    /** The S3Client shutdown logic does not handle shutdown of the HttpClient passed into it. So we must manually handle that. */
    private final SdkHttpClient httpClient;

    AmazonS3Reference(S3Client client, SdkHttpClient httpClient) {
        this.client = client;
        this.httpClient = httpClient;
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
    public S3Client client() {
        return client;
    }

    @Override
    protected void closeInternal() {
        client.close();
        httpClient.close();
    }

}
