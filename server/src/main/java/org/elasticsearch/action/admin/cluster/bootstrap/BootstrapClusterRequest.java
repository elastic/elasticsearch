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

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Request to set the initial configuration of master-eligible nodes in a cluster so that the very first master election can take place.
 */
public class BootstrapClusterRequest extends ActionRequest {
    private final BootstrapConfiguration bootstrapConfiguration;

    public BootstrapClusterRequest(BootstrapConfiguration bootstrapConfiguration) {
        this.bootstrapConfiguration = bootstrapConfiguration;
    }

    public BootstrapClusterRequest(StreamInput in) throws IOException {
        super(in);
        bootstrapConfiguration = new BootstrapConfiguration(in);
    }

    /**
     * @return the bootstrap configuration: the initial set of master-eligible nodes whose votes are counted in elections.
     */
    public BootstrapConfiguration getBootstrapConfiguration() {
        return bootstrapConfiguration;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        bootstrapConfiguration.writeTo(out);
    }
}
