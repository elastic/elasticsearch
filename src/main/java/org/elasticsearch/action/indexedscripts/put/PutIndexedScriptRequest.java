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

package org.elasticsearch.action.indexedscripts.put;

import com.google.common.base.Charsets;
import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.script.ScriptService;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Index request to index a script to the script index and make it available at search time.
 * <p/>
 * <p>The request requires the  {@link #scriptLang(String)}, {@link #id(String)} and
 * {@link #source(byte[])} to be set.
 * <p/>
 * <p>The source (content to index) can be set in its bytes form using ({@link #source()} (byte[])}),
 * its string form ({@link #source(String)}) or using a {@link org.elasticsearch.common.xcontent.XContentBuilder}
 * ({@link #source(org.elasticsearch.common.xcontent.XContentBuilder)}).
 * <p/>
 * <p>If the {@link #id(String)} is not set, it will be automatically generated.
 *
 * @see PutIndexedScriptResponse
 */
public class PutIndexedScriptRequest extends ActionRequest<PutIndexedScriptRequest> implements IndicesRequest {

    private String scriptLang;
    private String id;

    private BytesReference source;

    private IndexRequest.OpType opType = IndexRequest.OpType.INDEX;

    private long version = Versions.MATCH_ANY;
    private VersionType versionType = VersionType.INTERNAL;

    private XContentType contentType = Requests.INDEX_CONTENT_TYPE;

    public PutIndexedScriptRequest() {
        super();
    }

    /**
     * Constructs a new index request against the specific index and type. The
     * {@link #source(byte[])} must be set.
     */
    public PutIndexedScriptRequest(String scriptLang) {
        super();
        this.scriptLang = scriptLang;
    }

    /**
     * Constructs a new index request against the index, type, id and using the source.
     *
     * @param scriptLang  The scriptLang to index into
     * @param id    The id of document
     */
    public PutIndexedScriptRequest(String scriptLang, String id) {
        super();
        this.scriptLang = scriptLang;
        this.id = id;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (scriptLang == null) {
            validationException = addValidationError("scriptType is missing", validationException);
        }
        if (source == null) {
            validationException = addValidationError("source is missing", validationException);
        }
        if (id == null) {
            validationException = addValidationError("id is missing", validationException);
        }
        if (!versionType.validateVersionForWrites(version)) {
            validationException = addValidationError("illegal version value [" + version + "] for version type [" + versionType.name() + "]", validationException);
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
     * Sets the content type that will be used when generating a document from user provided objects (like Map).
     */
    public PutIndexedScriptRequest contentType(XContentType contentType) {
        this.contentType = contentType;
        return this;
    }

    /**
     * The type of the indexed document.
     */
    public String scriptLang() {
        return scriptLang;
    }

    /**
     * Sets the type of the indexed document.
     */
    public PutIndexedScriptRequest scriptLang(String scriptLang) {
        this.scriptLang = scriptLang;
        return this;
    }

    /**
     * The id of the indexed document. If not set, will be automatically generated.
     */
    public String id() {
        return id;
    }

    /**
     * Sets the id of the indexed document. If not set, will be automatically generated.
     */
    public PutIndexedScriptRequest id(String id) {
        this.id = id;
        return this;
    }

    /**
     * The source of the document to index, recopied to a new array if it is unsage.
     */
    public BytesReference source() {
        return source;
    }

    public Map<String, Object> sourceAsMap() {
        return XContentHelper.convertToMap(source, false).v2();
    }

    /**
     * Index the Map as a {@link org.elasticsearch.client.Requests#INDEX_CONTENT_TYPE}.
     *
     * @param source The map to index
     */
    public PutIndexedScriptRequest source(Map source) throws ElasticsearchGenerationException {
        return source(source, contentType);
    }

    /**
     * Index the Map as the provided content type.
     *
     * @param source The map to index
     */
    public PutIndexedScriptRequest source(Map source, XContentType contentType) throws ElasticsearchGenerationException {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            builder.map(source);
            return source(builder);
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    /**
     * Sets the document source to index.
     * <p/>
     * <p>Note, its preferable to either set it using {@link #source(org.elasticsearch.common.xcontent.XContentBuilder)}
     * or using the {@link #source(byte[])}.
     */
    public PutIndexedScriptRequest source(String source) {
        this.source = new BytesArray(source.getBytes(Charsets.UTF_8));
        return this;
    }

    /**
     * Sets the content source to index.
     */
    public PutIndexedScriptRequest source(XContentBuilder sourceBuilder) {
        source = sourceBuilder.bytes();
        return this;
    }

    public PutIndexedScriptRequest source(Object... source) {
        if (source.length % 2 != 0) {
            throw new IllegalArgumentException("The number of object passed must be even but was [" + source.length + "]");
        }
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            builder.startObject();
            for (int i = 0; i < source.length; i++) {
                builder.field(source[i++].toString(), source[i]);
            }
            builder.endObject();
            return source(builder);
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate", e);
        }
    }

    /**
     * Sets the document to index in bytes form.
     */
    public PutIndexedScriptRequest source(BytesReference source) {
        this.source = source;
        return this;
    }

    /**
     * Sets the document to index in bytes form.
     */
    public PutIndexedScriptRequest source(byte[] source) {
        return source(source, 0, source.length);
    }

    /**
     * Sets the document to index in bytes form (assumed to be safe to be used from different
     * threads).
     *
     * @param source The source to index
     * @param offset The offset in the byte array
     * @param length The length of the data
     */
    public PutIndexedScriptRequest source(byte[] source, int offset, int length) {
        return source(new BytesArray(source, offset, length));
    }

    /**
     * Sets the type of operation to perform.
     */
    public PutIndexedScriptRequest opType(IndexRequest.OpType opType) {
        this.opType = opType;
        return this;
    }

    /**
     * Set to <tt>true</tt> to force this index to use {@link org.elasticsearch.action.index.IndexRequest.OpType#CREATE}.
     */
    public PutIndexedScriptRequest create(boolean create) {
        if (create) {
            return opType(IndexRequest.OpType.CREATE);
        } else {
            return opType(IndexRequest.OpType.INDEX);
        }
    }

    /**
     * The type of operation to perform.
     */
    public IndexRequest.OpType opType() {
        return this.opType;
    }

    /**
     * Sets the version, which will cause the index operation to only be performed if a matching
     * version exists and no changes happened on the doc since then.
     */
    public PutIndexedScriptRequest version(long version) {
        this.version = version;
        return this;
    }

    public long version() {
        return this.version;
    }

    /**
     * Sets the versioning type. Defaults to {@link org.elasticsearch.index.VersionType#INTERNAL}.
     */
    public PutIndexedScriptRequest versionType(VersionType versionType) {
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
        id = in.readOptionalString();
        source = in.readBytesReference();

        opType = IndexRequest.OpType.fromId(in.readByte());
        version = in.readLong();
        versionType = VersionType.fromValue(in.readByte());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(scriptLang);
        out.writeOptionalString(id);
        out.writeBytesReference(source);
        out.writeByte(opType.id());
        out.writeLong(version);
        out.writeByte(versionType.getValue());
    }

    @Override
    public String toString() {
        String sSource = "_na_";
        try {
            sSource = XContentHelper.convertToJson(source, false);
        } catch (Exception e) {
            // ignore
        }
        return "index {[" + ScriptService.SCRIPT_INDEX + "][" + scriptLang + "][" + id + "], source[" + sSource + "]}";
    }
}
