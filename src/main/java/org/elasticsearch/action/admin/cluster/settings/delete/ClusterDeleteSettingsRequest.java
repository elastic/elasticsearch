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

package org.elasticsearch.action.admin.cluster.settings.delete;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.settings.ImmutableSettings.readSettingsFromStream;
import static org.elasticsearch.common.settings.ImmutableSettings.writeSettingsToStream;

/**
 * Request for an delete cluster settings action
 */
public class ClusterDeleteSettingsRequest extends AcknowledgedRequest<ClusterDeleteSettingsRequest> {

    /**
     * set to false to keep transient settings
     */
    private boolean deleteTransient = true;

    /**
     * set to false to keep persistent settings
     */
    private boolean deletePersistent = true;

    public ClusterDeleteSettingsRequest() {
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (!deleteTransient && !deletePersistent) {
            validationException = addValidationError("no settings to delete", validationException);
        }
        return validationException;
    }

    boolean deleteTransient() {
        return deleteTransient;
    }

    boolean deletePersistent() {
        return deletePersistent;
    }

    public ClusterDeleteSettingsRequest deleteTransient(boolean deleteTransient) {
        this.deleteTransient = deleteTransient;
        return this;
    }

    public ClusterDeleteSettingsRequest deletePersistent(boolean deletePersistent) {
        this.deletePersistent = deletePersistent;
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        deleteTransient = in.readBoolean();
        deletePersistent = in.readBoolean();
        readTimeout(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(deleteTransient);
        out.writeBoolean(deletePersistent);
        writeTimeout(out);
    }
}
