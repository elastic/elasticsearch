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

package org.elasticsearch.action.admin.indices.upgrade.post;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Request for an update index settings action
 */
public class UpgradeSettingsRequest extends AcknowledgedRequest<UpgradeSettingsRequest> {

    private Map<String, Tuple<Version, String>> versions;

    public UpgradeSettingsRequest(StreamInput in) throws IOException {
        super(in);
        versions = in.readMap(StreamInput::readString, i -> new Tuple<>(Version.readVersion(i), i.readString()));
    }

    public UpgradeSettingsRequest() {
    }

    /**
     * Constructs a new request to update minimum compatible version settings for one or more indices
     *
     * @param versions a map from index name to elasticsearch version, oldest lucene segment version tuple
     */
    public UpgradeSettingsRequest(Map<String, Tuple<Version, String>> versions) {
        this.versions = versions;
    }


    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (versions.isEmpty()) {
            validationException = addValidationError("no indices to update", validationException);
        }
        return validationException;
    }


    Map<String, Tuple<Version, String>> versions() {
        return versions;
    }

    /**
     * Sets the index versions to be updated
     */
    public UpgradeSettingsRequest versions(Map<String, Tuple<Version, String>> versions) {
        this.versions = versions;
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeMap(versions, StreamOutput::writeString, (o, v) -> {
            Version.writeVersion(v.v1(), out);
            out.writeString(v.v2());
        });
    }
}
