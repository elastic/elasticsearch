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

package org.elasticsearch.action.indexedscripts.delete;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.script.ScriptService;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A request to delete a script from the script index based on its scriptLang and id. Best created using
 * <p/>
 * <p>The operation requires the , {@link #scriptLang(String)} and {@link #id(String)} to
 * be set.
 *
 * @see DeleteIndexedScriptResponse
 * @see org.elasticsearch.client.Client#deleteIndexedScript(DeleteIndexedScriptRequest)
 */
public class DeleteIndexedScriptRequest extends ActionRequest<DeleteIndexedScriptRequest> implements IndicesRequest {

    private String scriptLang;
    private String id;
    private long version = Versions.MATCH_ANY;
    private VersionType versionType = VersionType.INTERNAL;


    public DeleteIndexedScriptRequest() {
    }

    /**
     * Constructs a new delete request against the specified index with the scriptLang and id.
     *
     * @param scriptLang  The scriptLang of the document
     * @param id    The id of the document
     */
    public DeleteIndexedScriptRequest(String scriptLang, String id) {
        this.scriptLang = scriptLang;
        this.id = id;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (scriptLang == null) {
            validationException = addValidationError("scriptLang is missing", validationException);
        }
        if (id == null) {
            validationException = addValidationError("id is missing", validationException);
        }
        if (!versionType.validateVersionForWrites(version)) {
            validationException = addValidationError("illegal version value [" + version + "] for version scriptLang [" + versionType.name() + "]", validationException);
        }
        return validationException;
    }

    @Override
    public String[] indices() {
        return new String[]{ScriptService.SCRIPT_INDEX};
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
    }

    /**
     * The scriptLang of the document to delete.
     */
    public String scriptLang() {
        return scriptLang;
    }

    /**
     * Sets the scriptLang of the document to delete.
     */
    public DeleteIndexedScriptRequest scriptLang(String type) {
        this.scriptLang = type;
        return this;
    }

    /**
     * The id of the document to delete.
     */
    public String id() {
        return id;
    }

    /**
     * Sets the id of the document to delete.
     */
    public DeleteIndexedScriptRequest id(String id) {
        this.id = id;
        return this;
    }

    /**
     * Sets the version, which will cause the delete operation to only be performed if a matching
     * version exists and no changes happened on the doc since then.
     */
    public DeleteIndexedScriptRequest version(long version) {
        this.version = version;
        return this;
    }

    public long version() {
        return this.version;
    }

    public DeleteIndexedScriptRequest versionType(VersionType versionType) {
        this.versionType = versionType;
        return this;
    }

    public VersionType versionType() {
        return this.versionType;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        scriptLang = in.readString();
        id = in.readString();
        version = in.readLong();
        versionType = VersionType.fromValue(in.readByte());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(scriptLang);
        out.writeString(id);
        out.writeLong(version);
        out.writeByte(versionType.getValue());
    }

    @Override
    public String toString() {
        return "delete {[" + ScriptService.SCRIPT_INDEX + "][" + scriptLang + "][" + id + "]}";
    }
}
