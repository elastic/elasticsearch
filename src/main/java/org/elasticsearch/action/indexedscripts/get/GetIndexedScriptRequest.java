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

package org.elasticsearch.action.indexedscripts.get;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.fetch.source.FetchSourceContext;

import java.io.IOException;

/**
 * A request to get an indexed script (its source) based on its language (optional) and id.
 * The operation requires the {@link #scriptLang(String)} and {@link #id(String)} to be set.
 *
 * @see GetIndexedScriptResponse
 */
public class GetIndexedScriptRequest extends ActionRequest<GetIndexedScriptRequest> implements IndicesRequest {

    protected String scriptLang;
    protected String id;
    protected String preference;
    protected String routing;
    private FetchSourceContext fetchSourceContext;

    private boolean refresh = false;

    Boolean realtime;

    private VersionType versionType = VersionType.INTERNAL;
    private long version = Versions.MATCH_ANY;

    /**
     * Constructs a new get request against the script index. The {@link #scriptLang(String)} and {@link #id(String)}
     * must be set.
     */
    public GetIndexedScriptRequest() {

    }

    /**
     * Constructs a new get request against the script index with the type and id.
     *
     * @param scriptLang  The language of the script
     * @param id    The id of the script
     */
    public GetIndexedScriptRequest(String scriptLang, String id) {
        this.scriptLang = scriptLang;
        this.id = id;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (scriptLang == null) {
            validationException = ValidateActions.addValidationError("type is missing", validationException);
        }
        if (id == null) {
            validationException = ValidateActions.addValidationError("id is missing", validationException);
        }
        if (!versionType.validateVersionForReads(version)) {
            validationException = ValidateActions.addValidationError("illegal version value [" + version + "] for version type [" + versionType.name() + "]",
                    validationException);
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
     * Sets the language of the script to fetch.
     */
    public GetIndexedScriptRequest scriptLang(@Nullable String type) {
        this.scriptLang = type;
        return this;
    }

    /**
     * Sets the id of the script to fetch.
     */
    public GetIndexedScriptRequest id(String id) {
        this.id = id;
        return this;
    }

    /**
     * Controls the shard routing of the request. Using this value to hash the shard
     * and not the id.
     */
    public GetIndexedScriptRequest routing(String routing) {
        this.routing = routing;
        return this;
    }

    /**
     * Sets the preference to execute the get. Defaults to randomize across shards. Can be set to
     * <tt>_local</tt> to prefer local shards, <tt>_primary</tt> to execute only on primary shards, or
     * a custom value, which guarantees that the same order will be used across different requests.
     */
    public GetIndexedScriptRequest preference(String preference) {
        this.preference = preference;
        return this;
    }

    public String scriptLang() {
        return scriptLang;
    }

    public String id() {
        return id;
    }

    public String routing() {
        return routing;
    }

    public String preference() {
        return this.preference;
    }

    /**
     * Should a refresh be executed before this get operation causing the operation to
     * return the latest value. Note, heavy get should not set this to <tt>true</tt>. Defaults
     * to <tt>false</tt>.
     */
    public GetIndexedScriptRequest refresh(boolean refresh) {
        this.refresh = refresh;
        return this;
    }

    public boolean refresh() {
        return this.refresh;
    }

    public boolean realtime() {
        return this.realtime == null ? true : this.realtime;
    }

    public GetIndexedScriptRequest realtime(Boolean realtime) {
        this.realtime = realtime;
        return this;
    }

    /**
     * Sets the version, which will cause the get operation to only be performed if a matching
     * version exists and no changes happened on the doc since then.
     */
    public long version() {
        return version;
    }

    public GetIndexedScriptRequest version(long version) {
        this.version = version;
        return this;
    }

    /**
     * Sets the versioning type. Defaults to {@link org.elasticsearch.index.VersionType#INTERNAL}.
     */
    public GetIndexedScriptRequest versionType(VersionType versionType) {
        this.versionType = versionType;
        return this;
    }

    public VersionType versionType() {
        return this.versionType;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        if (in.getVersion().before(Version.V_1_4_0_Beta1)) {
            //the index was previously serialized although not needed
            in.readString();
        }
        scriptLang = in.readString();
        id = in.readString();
        preference = in.readOptionalString();
        refresh = in.readBoolean();
        byte realtime = in.readByte();
        if (realtime == 0) {
            this.realtime = false;
        } else if (realtime == 1) {
            this.realtime = true;
        }

        this.versionType = VersionType.fromValue(in.readByte());
        this.version = Versions.readVersionWithVLongForBW(in);

        fetchSourceContext = FetchSourceContext.optionalReadFromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().before(Version.V_1_4_0_Beta1)) {
            //the index was previously serialized although not needed
            out.writeString(ScriptService.SCRIPT_INDEX);
        }
        out.writeString(scriptLang);
        out.writeString(id);
        out.writeOptionalString(preference);
        out.writeBoolean(refresh);
        if (realtime == null) {
            out.writeByte((byte) -1);
        } else if (!realtime) {
            out.writeByte((byte) 0);
        } else {
            out.writeByte((byte) 1);
        }

        out.writeByte(versionType.getValue());
        Versions.writeVersionWithVLongForBW(version, out);

        FetchSourceContext.optionalWriteToStream(fetchSourceContext, out);
    }

    @Override
    public String toString() {
        return "[" + ScriptService.SCRIPT_INDEX + "][" + scriptLang + "][" + id + "]: routing [" + routing + "]";
    }
}
