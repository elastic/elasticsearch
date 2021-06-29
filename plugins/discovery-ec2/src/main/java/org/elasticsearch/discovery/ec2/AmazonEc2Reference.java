/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery.ec2;

import com.amazonaws.services.ec2.AmazonEC2;

import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.AbstractRefCounted;

/**
 * Handles the shutdown of the wrapped {@link AmazonEC2} using reference
 * counting.
 */
public class AmazonEc2Reference extends AbstractRefCounted implements Releasable {

    private final AmazonEC2 client;

    AmazonEc2Reference(AmazonEC2 client) {
        super("AWS_EC2_CLIENT");
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
     * Returns the underlying `AmazonEC2` client. All method calls are permitted BUT
     * NOT shutdown. Shutdown is called when reference count reaches 0.
     */
    public AmazonEC2 client() {
        return client;
    }

    @Override
    protected void closeInternal() {
        client.shutdown();
    }

}
