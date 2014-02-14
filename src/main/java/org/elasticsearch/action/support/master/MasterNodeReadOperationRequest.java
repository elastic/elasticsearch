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

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Base request for master based read operations that allows to read the cluster state from the local node if needed
 */
public abstract class MasterNodeReadOperationRequest<T extends MasterNodeReadOperationRequest> extends MasterNodeOperationRequest<T> {

    protected boolean local = false;

    @SuppressWarnings("unchecked")
    public final T local(boolean local) {
        this.local = local;
        return (T) this;
    }

    public final boolean local() {
        return local;
    }

    /**
     * Reads the local flag
     */
    protected void readLocal(StreamInput in) throws IOException {
        readLocal(in, null);
    }

    /**
     * Reads the local flag if on or after the specified min version or if the version is <code>null</code>.
     */
    protected void readLocal(StreamInput in, Version minVersion) throws IOException {
        if (minVersion == null || in.getVersion().onOrAfter(minVersion)) {
            local = in.readBoolean();
        }
    }

    /**
     * writes the local flag
     */
    protected void writeLocal(StreamOutput out) throws IOException {
        writeLocal(out, null);
    }

    /**
     * writes the local flag if on or after the specified min version or if the version is <code>null</code>.
     */
    protected void writeLocal(StreamOutput out, Version minVersion) throws IOException {
        if (minVersion == null || out.getVersion().onOrAfter(minVersion)) {
            out.writeBoolean(local);
        }
    }
}
