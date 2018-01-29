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

package org.elasticsearch.action.admin.cluster.reinit;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Request for an update cluster settings action
 */
public class NodesReInitRequest extends BaseNodesRequest<NodesReInitRequest> {

    private String secureStorePassword;

    public NodesReInitRequest() {
    }
    
    /**
     * Get usage from nodes based on the nodes ids specified. If none are
     * passed, usage for all nodes will be returned.
     */
    public NodesReInitRequest(String... nodesIds) {
        super(nodesIds);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (secureStorePassword == null) {
            validationException = addValidationError("secure store password cannot be null (use empty string).", validationException);
        }
        return validationException;
    }

    public String secureStorePassword() {
        return secureStorePassword;
    }

    public NodesReInitRequest secureStorePassword(String secureStorePassword) {
        this.secureStorePassword = secureStorePassword;
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        secureStorePassword = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(secureStorePassword);
    }
}
