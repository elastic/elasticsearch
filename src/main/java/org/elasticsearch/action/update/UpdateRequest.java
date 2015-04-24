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

package org.elasticsearch.action.update;

import com.google.common.collect.Maps;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.DocumentRequest;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.single.instance.InstanceShardOperationRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.script.ScriptParameterParser;
import org.elasticsearch.script.ScriptParameterParser.ScriptParameterValue;
import org.elasticsearch.script.ScriptService;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 */
public class UpdateRequest extends InstanceShardOperationRequest<UpdateRequest> implements DocumentRequest<UpdateRequest> {

    private String type;
    private String id;
    @Nullable
    private String routing;

    @Nullable
    private String parent;

    @Nullable
    String script;
    @Nullable
    ScriptService.ScriptType scriptType;
    @Nullable
    String scriptLang;
    @Nullable
    Map<String, Object> scriptParams;

    private String[] fields;

    private long version = Versions.MATCH_ANY;
    private VersionType versionType = VersionType.INTERNAL;
    private int retryOnConflict = 0;

    private boolean refresh = false;

    private WriteConsistencyLevel consistencyLevel = WriteConsistencyLevel.DEFAULT;

    private IndexRequest upsertRequest;

    private boolean scriptedUpsert = false;
    private boolean docAsUpsert = false;
    private boolean detectNoop = false;

    @Nullable
    private IndexRequest doc;

    public UpdateRequest() {

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

        if (!(versionType == VersionType.INTERNAL || versionType == VersionType.FORCE)) {
            validationException = addValidationError("version type [" + versionType + "] is not supported by the update API", validationException);
        } else {

            if (version != Versions.MATCH_ANY && retryOnConflict > 0) {
                validationException = addValidationError("can't provide both retry_on_conflict and a specific version", validationException);
            }

            if (!versionType.validateVersionForWrites(version)) {
                validationException = addValidationError("illegal version value [" + version + "] for version type [" + versionType.name() + "]", validationException);
            }
        }

        if (script == null && doc == null) {
            validationException = addValidationError("script or doc is missing", validationException);
        }
        if (script != null && doc != null) {
            validationException = addValidationError("can't provide both script and doc", validationException);
        }
        if (doc == null && docAsUpsert) {
            validationException = addValidationError("doc must be specified if doc_as_upsert is enabled", validationException);
        }
        return validationException;
    }

    /**
     * The type of the indexed document.
     */
    @Override
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
    @Override
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
    @Override
    public UpdateRequest routing(String routing) {
        if (routing != null && routing.length() == 0) {
            this.routing = null;
        } else {
            this.routing = routing;
        }
        return this;
    }

    /**
     * Controls the shard routing of the request. Using this value to hash the shard
     * and not the id.
     */
    @Override
    public String routing() {
        return this.routing;
    }

    /**
     * The parent id is used for the upsert request and also implicitely sets the routing if not already set.
     */
    public UpdateRequest parent(String parent) {
        this.parent = parent;
        if (routing == null) {
            routing = parent;
        }
        return this;
    }

    public String parent() {
        return parent;
    }

    int shardId() {
        return this.shardId;
    }

    public String script() {
        return this.script;
    }

    public ScriptService.ScriptType scriptType() { return this.scriptType; }

    public Map<String, Object> scriptParams() {
        return this.scriptParams;
    }

    /**
     * The script to execute. Note, make sure not to send different script each times and instead
     * use script params if possible with the same (automatically compiled) script.
     */
    public UpdateRequest script(String script, ScriptService.ScriptType scriptType) {
        this.script = script;
        this.scriptType = scriptType;
        return this;
    }

    /**
     * The script to execute. Note, make sure not to send different script each times and instead
     * use script params if possible with the same (automatically compiled) script.
     */
    public UpdateRequest script(String script) {
        this.script = script;
        this.scriptType = ScriptService.ScriptType.INLINE;
        return this;
    }


    /**
     * The language of the script to execute.
     */
    public UpdateRequest scriptLang(String scriptLang) {
        this.scriptLang = scriptLang;
        return this;
    }

    public String scriptLang() {
        return scriptLang;
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
    public UpdateRequest script(String script, ScriptService.ScriptType scriptType, @Nullable Map<String, Object> scriptParams) {
        this.script = script;
        this.scriptType = scriptType;
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
     * @param scriptType   The script type
     * @param scriptParams The script parameters
     */
    public UpdateRequest script(String script, @Nullable String scriptLang, ScriptService.ScriptType scriptType, @Nullable Map<String, Object> scriptParams) {
        this.script = script;
        this.scriptLang = scriptLang;
        this.scriptType = scriptType;
        if (this.scriptParams != null) {
            this.scriptParams.putAll(scriptParams);
        } else {
            this.scriptParams = scriptParams;
        }
        return this;
    }

    /**
     * Explicitly specify the fields that will be returned. By default, nothing is returned.
     */
    public UpdateRequest fields(String... fields) {
        this.fields = fields;
        return this;
    }

    /**
     * Get the fields to be returned.
     */
    public String[] fields() {
        return this.fields;
    }

    /**
     * Sets the number of retries of a version conflict occurs because the document was updated between
     * getting it and updating it. Defaults to 0.
     */
    public UpdateRequest retryOnConflict(int retryOnConflict) {
        this.retryOnConflict = retryOnConflict;
        return this;
    }

    public int retryOnConflict() {
        return this.retryOnConflict;
    }

    /**
     * Sets the version, which will cause the index operation to only be performed if a matching
     * version exists and no changes happened on the doc since then.
     */
    public UpdateRequest version(long version) {
        this.version = version;
        return this;
    }

    public long version() {
        return this.version;
    }

    /**
     * Sets the versioning type. Defaults to {@link VersionType#INTERNAL}.
     */
    public UpdateRequest versionType(VersionType versionType) {
        this.versionType = versionType;
        return this;
    }

    public VersionType versionType() {
        return this.versionType;
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

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public UpdateRequest doc(IndexRequest doc) {
        this.doc = doc;
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public UpdateRequest doc(XContentBuilder source) {
        safeDoc().source(source);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public UpdateRequest doc(Map source) {
        safeDoc().source(source);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public UpdateRequest doc(Map source, XContentType contentType) {
        safeDoc().source(source, contentType);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public UpdateRequest doc(String source) {
        safeDoc().source(source);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public UpdateRequest doc(byte[] source) {
        safeDoc().source(source);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public UpdateRequest doc(byte[] source, int offset, int length) {
        safeDoc().source(source, offset, length);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified, the doc provided
     * is a field and value pairs.
     */
    public UpdateRequest doc(Object... source) {
        safeDoc().source(source);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public UpdateRequest doc(String field, Object value) {
        safeDoc().source(field, value);
        return this;
    }

    public IndexRequest doc() {
        return this.doc;
    }

    private IndexRequest safeDoc() {
        if (doc == null) {
            doc = new IndexRequest();
        }
        return doc;
    }

    /**
     * Sets the index request to be used if the document does not exists. Otherwise, a {@link org.elasticsearch.index.engine.DocumentMissingException}
     * is thrown.
     */
    public UpdateRequest upsert(IndexRequest upsertRequest) {
        this.upsertRequest = upsertRequest;
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists.
     */
    public UpdateRequest upsert(XContentBuilder source) {
        safeUpsertRequest().source(source);
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists.
     */
    public UpdateRequest upsert(Map source) {
        safeUpsertRequest().source(source);
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists.
     */
    public UpdateRequest upsert(Map source, XContentType contentType) {
        safeUpsertRequest().source(source, contentType);
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists.
     */
    public UpdateRequest upsert(String source) {
        safeUpsertRequest().source(source);
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists.
     */
    public UpdateRequest upsert(byte[] source) {
        safeUpsertRequest().source(source);
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists.
     */
    public UpdateRequest upsert(byte[] source, int offset, int length) {
        safeUpsertRequest().source(source, offset, length);
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists. The doc
     * includes field and value pairs.
     */
    public UpdateRequest upsert(Object... source) {
        safeUpsertRequest().source(source);
        return this;
    }

    public IndexRequest upsertRequest() {
        return this.upsertRequest;
    }

    private IndexRequest safeUpsertRequest() {
        if (upsertRequest == null) {
            upsertRequest = new IndexRequest();
        }
        return upsertRequest;
    }

    public UpdateRequest source(XContentBuilder source) throws Exception {
        return source(source.bytes());
    }

    public UpdateRequest source(byte[] source) throws Exception {
        return source(source, 0, source.length);
    }

    public UpdateRequest source(byte[] source, int offset, int length) throws Exception {
        return source(new BytesArray(source, offset, length));
    }

    /**
     * Should this update attempt to detect if it is a noop?
     * @return this for chaining
     */
    public UpdateRequest detectNoop(boolean detectNoop) {
        this.detectNoop = detectNoop;
        return this;
    }

    public boolean detectNoop() {
        return detectNoop;
    }

    public UpdateRequest source(BytesReference source) throws Exception {
        ScriptParameterParser scriptParameterParser = new ScriptParameterParser();
        XContentType xContentType = XContentFactory.xContentType(source);
        try (XContentParser parser = XContentFactory.xContent(xContentType).createParser(source)) {
            XContentParser.Token token = parser.nextToken();
            if (token == null) {
                return this;
            }
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if ("params".equals(currentFieldName)) {
                    scriptParams = parser.map();
                } else if ("scripted_upsert".equals(currentFieldName)) {
                    scriptedUpsert = parser.booleanValue();
                } else if ("upsert".equals(currentFieldName)) {
                    XContentBuilder builder = XContentFactory.contentBuilder(xContentType);
                    builder.copyCurrentStructure(parser);
                    safeUpsertRequest().source(builder);
                } else if ("doc".equals(currentFieldName)) {
                    XContentBuilder docBuilder = XContentFactory.contentBuilder(xContentType);
                    docBuilder.copyCurrentStructure(parser);
                    safeDoc().source(docBuilder);
                } else if ("doc_as_upsert".equals(currentFieldName)) {
                    docAsUpsert(parser.booleanValue());
                } else if ("detect_noop".equals(currentFieldName)) {
                    detectNoop(parser.booleanValue());
                } else {
                    scriptParameterParser.token(currentFieldName, token, parser);
                }
            }
            ScriptParameterValue scriptValue = scriptParameterParser.getDefaultScriptParameterValue();
            if (scriptValue != null) {
                script = scriptValue.script();
                scriptType = scriptValue.scriptType();
            }
            scriptLang = scriptParameterParser.lang();
        }
        return this;
    }

    public boolean docAsUpsert() {
        return this.docAsUpsert;
    }

    public void docAsUpsert(boolean shouldUpsertDoc) {
        this.docAsUpsert = shouldUpsertDoc;
    }
    
    public boolean scriptedUpsert(){
        return this.scriptedUpsert;
    }
    
    public void scriptedUpsert(boolean scriptedUpsert) {
        this.scriptedUpsert = scriptedUpsert;
    }
    

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        consistencyLevel = WriteConsistencyLevel.fromId(in.readByte());
        type = in.readString();
        id = in.readString();
        routing = in.readOptionalString();
        parent = in.readOptionalString();
        script = in.readOptionalString();
        if(Strings.hasLength(script)) {
            scriptType = ScriptService.ScriptType.readFrom(in);
        }
        scriptLang = in.readOptionalString();
        scriptParams = in.readMap();
        retryOnConflict = in.readVInt();
        refresh = in.readBoolean();
        if (in.readBoolean()) {
            doc = new IndexRequest();
            doc.readFrom(in);
        }
        int size = in.readInt();
        if (size >= 0) {
            fields = new String[size];
            for (int i = 0; i < size; i++) {
                fields[i] = in.readString();
            }
        }
        if (in.readBoolean()) {
            upsertRequest = new IndexRequest();
            upsertRequest.readFrom(in);
        }
        docAsUpsert = in.readBoolean();
        version = in.readLong();
        versionType = VersionType.fromValue(in.readByte());
        detectNoop = in.readBoolean();
        scriptedUpsert = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeByte(consistencyLevel.id());
        out.writeString(type);
        out.writeString(id);
        out.writeOptionalString(routing);
        out.writeOptionalString(parent);
        out.writeOptionalString(script);
        if (Strings.hasLength(script)) {
            ScriptService.ScriptType.writeTo(scriptType, out);
        }
        out.writeOptionalString(scriptLang);
        out.writeMap(scriptParams);
        out.writeVInt(retryOnConflict);
        out.writeBoolean(refresh);
        if (doc == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            // make sure the basics are set
            doc.index(index);
            doc.type(type);
            doc.id(id);
            doc.writeTo(out);
        }
        if (fields == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(fields.length);
            for (String field : fields) {
                out.writeString(field);
            }
        }
        if (upsertRequest == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            // make sure the basics are set
            upsertRequest.index(index);
            upsertRequest.type(type);
            upsertRequest.id(id);
            upsertRequest.writeTo(out);
        }
        out.writeBoolean(docAsUpsert);
        out.writeLong(version);
        out.writeByte(versionType.getValue());
        out.writeBoolean(detectNoop);
        out.writeBoolean(scriptedUpsert);
    }

}
