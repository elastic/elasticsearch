/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.discovery.ec2;

import software.amazon.awssdk.services.ec2.Ec2Client;

import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Releasable;

/**
 * Handles the shutdown of the wrapped {@link Ec2Client} using reference counting.
 */
public class AmazonEc2Reference extends AbstractRefCounted implements Releasable {

    private final Ec2Client client;

    AmazonEc2Reference(Ec2Client client) {
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
     * Returns the underlying {@link Ec2Client} client. All method calls are permitted EXCEPT {@link Ec2Client#close}, which is called
     * automatically when this object's reference count reaches 0.
     */
    public Ec2Client client() {
        return client;
    }

    @Override
    protected void closeInternal() {
        client.close();
    }

}
