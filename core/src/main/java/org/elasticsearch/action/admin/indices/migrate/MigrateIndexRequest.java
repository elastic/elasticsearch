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
package org.elasticsearch.action.admin.indices.migrate;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.script.Script;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Request to migrate data from one index into a new index.
 */
public class MigrateIndexRequest extends AcknowledgedRequest<MigrateIndexRequest> implements IndicesRequest {
    private String sourceIndex;
    private CreateIndexRequest createIndexRequest;
    private Script script;

    /**
     * Build an empty request.
     */
    public MigrateIndexRequest() {}

    /**
     * Build a fully defined request.
     */
    public MigrateIndexRequest(String sourceIndex, String newIndex) {
        this.sourceIndex = sourceIndex;
        this.createIndexRequest = new CreateIndexRequest(newIndex);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (sourceIndex == null) {
            validationException = addValidationError("source index is not set", validationException);
        }
        if (createIndexRequest == null) {
            validationException = addValidationError("create index request is not set", validationException);
        } else {
            ActionRequestValidationException createValidation = createIndexRequest.validate();
            if (createValidation != null) {
                for (String createValidationError: createValidation.validationErrors()) {
                    validationException = addValidationError("validation error with create index: " + createValidationError,
                            validationException);
                }
            }
            if (Objects.equals(sourceIndex, createIndexRequest.index())) {
                validationException = addValidationError("source and destination can't be the same index", validationException);
            }
            if (createIndexRequest.aliases().isEmpty()) {
                validationException = addValidationError("migrating an index requires an alias", validationException);
            }
            for (Alias alias : createIndexRequest.aliases()) {
                if (Objects.equals(createIndexRequest.index(), alias.name())) {
                    validationException = addValidationError(
                            "can't add an alias with the same name as the destination index [" + createIndexRequest.index() + "]",
                            validationException);
                }
            }
            if (ActiveShardCount.NONE.equals(createIndexRequest.waitForActiveShards())) {
                validationException = addValidationError("must wait for more than one active shard in the new index", validationException);
            }
        }
        
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        readTimeout(in);
        sourceIndex = in.readString();
        createIndexRequest = new CreateIndexRequest();
        createIndexRequest.readFrom(in);
        script = in.readOptionalWriteable(Script::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        writeTimeout(out);
        out.writeString(sourceIndex);
        createIndexRequest.writeTo(out);
        out.writeOptionalWriteable(script);
    }

    @Override
    public MigrateIndexTask createTask(long id, String type, String action, TaskId parentTaskId) {
        return new MigrateIndexTask(id, type, action, getDescription(), getParentTask());
    }

    /**
     * Set the index to migrate from.
     */
    public void setSourceIndex(String sourceIndex) {
        this.sourceIndex = sourceIndex;
    }

    /**
     * The index to migrate from.
     */
    public String getSourceIndex() {
        return sourceIndex;
    }

    /**
     * Set the script used to migrate documents.
     */
    public void setScript(Script script) {
        this.script = script;
    }

    /**
     * The script used to migrate documents.
     */
    public Script getScript() {
        return script;
    }

    /**
     * Set the request used to create the new index.
     */
    public void setCreateIndexRequest(CreateIndexRequest createIndexRequest) {
        this.createIndexRequest = createIndexRequest;
    }

    /**
     * The request used to create the new index.
     */
    public CreateIndexRequest getCreateIndexRequest() {
        return createIndexRequest;
    }

    @Override
    public String[] indices() {
        return new String[] { sourceIndex, createIndexRequest.index() };
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
    }

    @Override
    public String toString() {
        return "MigrateIndex["
                + "source=" + sourceIndex
                + ",create=" + createIndexRequest
                + ",script=" + script
                + ",timeout=" + timeout
                + ",masterNodeTimeout=" + masterNodeTimeout
                + ",parentTask=" + getParentTask() + "]";
    }

    @Override
    public boolean equals(Object obj) {
        if (getClass() != obj.getClass()) return false;
        MigrateIndexRequest other = (MigrateIndexRequest) obj;
        return Objects.equals(sourceIndex, other.sourceIndex)
                && Objects.equals(createIndexRequest, other.createIndexRequest)
                && Objects.equals(script, other.script)
                && Objects.equals(timeout, other.timeout)
                && Objects.equals(masterNodeTimeout, other.masterNodeTimeout)
                && Objects.equals(getParentTask(), other.getParentTask());
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceIndex, script, createIndexRequest, timeout, masterNodeTimeout, getParentTask());
    }
}
