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
package org.elasticsearch.action.admin.cluster.bootstrap;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Response to a {@link BootstrapClusterRequest} indicating that the cluster has been successfully bootstrapped.
 */
public class BootstrapClusterResponse extends ActionResponse {
    private final boolean alreadyBootstrapped;

    public BootstrapClusterResponse(boolean alreadyBootstrapped) {
        this.alreadyBootstrapped = alreadyBootstrapped;
    }

    public BootstrapClusterResponse(StreamInput in) throws IOException {
        super(in);
        alreadyBootstrapped = in.readBoolean();
    }

    /**
     * @return whether this node already knew that the cluster had been bootstrapped when handling this request.
     */
    public boolean getAlreadyBootstrapped() {
        return alreadyBootstrapped;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(alreadyBootstrapped);
    }

    @Override
    public String toString() {
        return "BootstrapClusterResponse{" +
            "alreadyBootstrapped=" + alreadyBootstrapped +
            '}';
    }
}
