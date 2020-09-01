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
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;

public class CloneSnapshotRequest extends MasterNodeRequest<CloneSnapshotRequest> implements IndicesRequest.Replaceable {

    private final String repository;

    private final String source;

    private final String target;

    private String[] indices;

    private IndicesOptions indicesOptions = IndicesOptions.strictExpandHidden();

    private final String[] excludedSettings;

    private Settings indexSettings;

    public CloneSnapshotRequest(StreamInput in) throws IOException {
        super(in);
        repository = in.readString();
        source = in.readString();
        target = in.readString();
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        excludedSettings = in.readStringArray();
        indexSettings = Settings.readSettingsFromStream(in);
    }

    public CloneSnapshotRequest(String repository, String source, String target, String[] indices, String[] excludedSettings,
                                Settings indexSettings) {
        this.repository = repository;
        this.source = source;
        this.target = target;
        this.indices = indices;
        this.excludedSettings = excludedSettings;
        this.indexSettings = indexSettings;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(repository);
        out.writeString(source);
        out.writeString(target);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
        out.writeStringArray(excludedSettings);
        Settings.writeSettingsToStream(indexSettings, out);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public String[] indices() {
        return this.indices;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    public CloneSnapshotRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    @Override
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

    public Settings indexSettings() {
        return indexSettings;
    }

    public CloneSnapshotRequest indexSettings(Settings indexSettings) {
        this.indexSettings = indexSettings;
        return this;
    }
}
