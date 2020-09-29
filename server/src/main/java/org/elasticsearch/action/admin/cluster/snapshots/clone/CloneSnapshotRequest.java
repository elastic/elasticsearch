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

package org.elasticsearch.action.admin.cluster.snapshots.clone;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class CloneSnapshotRequest extends MasterNodeRequest<CloneSnapshotRequest> {

    private final String repository;

    private final String source;

    private final String target;

    // TODO: the current logic does not allow for specifying index resolution parameters like hidden and such. Do we care about cloning
    //        system or hidden indices?
    private String[] indices;

    public CloneSnapshotRequest(StreamInput in) throws IOException {
        super(in);
        repository = in.readString();
        source = in.readString();
        target = in.readString();
        indices = in.readStringArray();
    }

    public CloneSnapshotRequest(String repository, String source, String target, String[] indices) {
        this.repository = repository;
        this.source = source;
        this.target = target;
        this.indices = indices;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(repository);
        out.writeString(source);
        out.writeString(target);
        out.writeStringArray(indices);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public String[] indices() {
        return this.indices;
    }

    public CloneSnapshotRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    public String repository() {
        return this.repository;
    }

    public String target() {
        return this.target;
    }

    public String source() {
        return this.source;
    }
}
