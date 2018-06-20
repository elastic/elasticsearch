/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.discovery.ec2;

import com.amazonaws.services.ec2.AmazonEC2;

import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;

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
