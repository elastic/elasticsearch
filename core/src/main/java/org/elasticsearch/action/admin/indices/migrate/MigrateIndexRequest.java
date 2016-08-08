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
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseFieldMatcherSupplier;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.script.Script;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Request to migrate data from one index into a new index.
 */
public class MigrateIndexRequest extends AcknowledgedRequest<MigrateIndexRequest> implements IndicesRequest {

    public static final ObjectParser<MigrateIndexRequest, ParseFieldMatcherSupplier> PARSER = new ObjectParser<>("migrate", null);
    static {

    }

    private String sourceIndex;
    private Script script;
    private CreateIndexRequest createIndexRequest;

    /**
     * Constructor for serialization and {@link MigrateIndexRequestBuilder}.
     */
    MigrateIndexRequest() {}

    public MigrateIndexRequest(String sourceIndex, @Nullable Script script, CreateIndexRequest createIndexRequest) {
        this.sourceIndex = sourceIndex;
        this.script = script;
        this.createIndexRequest = createIndexRequest;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = createIndexRequest == null ? null : createIndexRequest.validate();
        if (sourceIndex == null) {
            validationException = addValidationError("source index is missing", validationException);
        }
        if (createIndexRequest == null) {
            validationException = addValidationError("create index request is missing", validationException);
        }
        // NOCOMMIT validate wait_for_active_shards is at least 1.
        
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        sourceIndex = in.readString();
        script = in.readOptionalWriteable(Script::new);
        createIndexRequest = new CreateIndexRequest();
        createIndexRequest.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(sourceIndex);
        out.writeOptionalWriteable(script);
        createIndexRequest.writeTo(out);
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId) {
        return new MigrateIndexTask(id, type, action, getDescription(), getParentTask(), this);
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
     * @return
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
}
