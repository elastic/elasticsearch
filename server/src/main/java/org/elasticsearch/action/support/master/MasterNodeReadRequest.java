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

package org.elasticsearch.action.support.master;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Base request for master based read operations that allows to read the cluster state from the local node if needed
 */
public abstract class MasterNodeReadRequest<Request extends MasterNodeReadRequest<Request>> extends MasterNodeRequest<Request> {

    protected boolean local = false;

    protected MasterNodeReadRequest() {
    }

    protected MasterNodeReadRequest(StreamInput in) throws IOException {
        super(in);
        local = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(local);
    }

    @SuppressWarnings("unchecked")
    public final Request local(boolean local) {
        this.local = local;
        return (Request) this;
    }

    /**
     * Return local information, do not retrieve the state from master node (default: false).
     * @return <code>true</code> if local information is to be returned;
     * <code>false</code> if information is to be retrieved from master node (default).
     */
    public final boolean local() {
        return local;
    }
}
