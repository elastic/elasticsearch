/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.action.update;

import com.google.common.collect.Maps;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.action.support.single.instance.InstanceShardOperationRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 */
public class UpdateRequest extends InstanceShardOperationRequest {

    private String type;
    private String id;
    @Nullable
    private String routing;

    String script;
    @Nullable
    String scriptLang;
    @Nullable
    Map<String, Object> scriptParams;

    int retryOnConflict = 0;

    private String percolate;

    private boolean refresh = false;

    private ReplicationType replicationType = ReplicationType.DEFAULT;
    private WriteConsistencyLevel consistencyLevel = WriteConsistencyLevel.DEFAULT;

    UpdateRequest() {

    }

    public UpdateRequest(String index, String type, String id) {
        this.index = index;
        this.type = type;
        this.id = id;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (type == null) {
            validationException = addValidationError("type is missing", validationException);
        }
        if (id == null) {
            validationException = addValidationError("id is missing", validationException);
        }
        if (script == null) {
            validationException = addValidationError("script is missing", validationException);
        }
        return validationException;
    }

    /**
     * Sets the index the document will exists on.
     */
    public UpdateRequest index(String index) {
        this.index = index;
        return this;
    }

    /**
     * The type of the indexed document.
     */
    public String type() {
        return type;
    }

    /**
     * Sets the type of the indexed document.
     */
    public UpdateRequest type(String type) {
        this.type = type;
        return this;
    }

    /**
     * The id of the indexed document.
     */
    public String id() {
        return id;
    }

    /**
     * Sets the id of the indexed document.
     */
    public UpdateRequest id(String id) {
        this.id = id;
        return this;
    }

    /**
     * Controls the shard routing of the request. Using this value to hash the shard
     * and not the id.
     */
    public UpdateRequest routing(String routing) {
        if (routing != null && routing.length() == 0) {
            this.routing = null;
        } else {
            this.routing = routing;
        }
        return this;
    }

    /**
     * Sets the parent id of this document. Will simply set the routing to this value, as it is only
     * used for routing with delete requests.
     */
    public UpdateRequest parent(String parent) {
        if (routing == null) {
            routing = parent;
        }
        return this;
    }

    /**
     * Controls the shard routing of the request. Using this value to hash the shard
     * and not the id.
     */
    public String routing() {
        return this.routing;
    }

    int shardId() {
        return this.shardId;
    }

    /**
     * The script to execute. Note, make sure not to send different script each times and instead
     * use script params if possible with the same (automatically compiled) script.
     */
    public UpdateRequest script(String script) {
        this.script = script;
        return this;
    }

    /**
     * The language of the script to execute.
     */
    public UpdateRequest scriptLang(String scriptLang) {
        this.scriptLang = scriptLang;
        return this;
    }

    /**
     * Add a script parameter.
     */
    public UpdateRequest addScriptParam(String name, Object value) {
        if (scriptParams == null) {
            scriptParams = Maps.newHashMap();
        }
        scriptParams.put(name, value);
        return this;
    }

    /**
     * Sets the script parameters to use with the script.
     */
    public UpdateRequest scriptParams(Map<String, Object> scriptParams) {
        if (this.scriptParams == null) {
            this.scriptParams = scriptParams;
        } else {
            this.scriptParams.putAll(scriptParams);
        }
        return this;
    }

    /**
     * The script to execute. Note, make sure not to send different script each times and instead
     * use script params if possible with the same (automatically compiled) script.
     */
    public UpdateRequest script(String script, @Nullable Map<String, Object> scriptParams) {
        this.script = script;
        if (this.scriptParams != null) {
            this.scriptParams.putAll(scriptParams);
        } else {
            this.scriptParams = scriptParams;
        }
        return this;
    }

    /**
     * The script to execute. Note, make sure not to send different script each times and instead
     * use script params if possible with the same (automatically compiled) script.
     *
     * @param script       The script to execute
     * @param scriptLang   The script language
     * @param scriptParams The script parameters
     */
    public UpdateRequest script(String script, @Nullable String scriptLang, @Nullable Map<String, Object> scriptParams) {
        this.script = script;
        this.scriptLang = scriptLang;
        if (this.scriptParams != null) {
            this.scriptParams.putAll(scriptParams);
        } else {
            this.scriptParams = scriptParams;
        }
        return this;
    }

    /**
     * Sets the number of retries of a version conflict occurs because the document was updated between
     * getting it and updating it. Defaults to 1.
     */
    public UpdateRequest retryOnConflict(int retryOnConflict) {
        this.retryOnConflict = retryOnConflict;
        return this;
    }

    public int retryOnConflict() {
        return this.retryOnConflict;
    }

    /**
     * Causes the update request document to be percolated. The parameter is the percolate query
     * to use to reduce the percolated queries that are going to run against this doc. Can be
     * set to <tt>*</tt> to indicate that all percolate queries should be run.
     */
    public UpdateRequest percolate(String percolate) {
        this.percolate = percolate;
        return this;
    }

    public String percolate() {
        return this.percolate;
    }

    /**
     * A timeout to wait if the index operation can't be performed immediately. Defaults to <tt>1m</tt>.
     */
    public UpdateRequest timeout(TimeValue timeout) {
        this.timeout = timeout;
        return this;
    }

    /**
     * A timeout to wait if the index operation can't be performed immediately. Defaults to <tt>1m</tt>.
     */
    public UpdateRequest timeout(String timeout) {
        return timeout(TimeValue.parseTimeValue(timeout, null));
    }

    /**
     * Should a refresh be executed post this update operation causing the operation to
     * be searchable. Note, heavy indexing should not set this to <tt>true</tt>. Defaults
     * to <tt>false</tt>.
     */
    public UpdateRequest refresh(boolean refresh) {
        this.refresh = refresh;
        return this;
    }

    public boolean refresh() {
        return this.refresh;
    }

    /**
     * The replication type.
     */
    public ReplicationType replicationType() {
        return this.replicationType;
    }

    /**
     * Sets the replication type.
     */
    public UpdateRequest replicationType(ReplicationType replicationType) {
        this.replicationType = replicationType;
        return this;
    }

    public WriteConsistencyLevel consistencyLevel() {
        return this.consistencyLevel;
    }

    /**
     * Sets the consistency level of write. Defaults to {@link org.elasticsearch.action.WriteConsistencyLevel#DEFAULT}
     */
    public UpdateRequest consistencyLevel(WriteConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        replicationType = ReplicationType.fromId(in.readByte());
        consistencyLevel = WriteConsistencyLevel.fromId(in.readByte());
        type = in.readUTF();
        id = in.readUTF();
        if (in.readBoolean()) {
            routing = in.readUTF();
        }
        script = in.readUTF();
        if (in.readBoolean()) {
            scriptLang = in.readUTF();
        }
        scriptParams = in.readMap();
        retryOnConflict = in.readVInt();
        if (in.readBoolean()) {
            percolate = in.readUTF();
        }
        refresh = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeByte(replicationType.id());
        out.writeByte(consistencyLevel.id());
        out.writeUTF(type);
        out.writeUTF(id);
        if (routing == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(routing);
        }
        out.writeUTF(script);
        if (scriptLang == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(scriptLang);
        }
        out.writeMap(scriptParams);
        out.writeVInt(retryOnConflict);
        if (percolate == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(percolate);
        }
        out.writeBoolean(refresh);
    }
}
