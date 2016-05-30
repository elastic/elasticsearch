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

package org.elasticsearch.action.get;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.RealtimeRequest;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.search.fetch.source.FetchSourceContext;

import java.io.IOException;

/**
 * A request to get a document (its source) from an index based on its type (optional) and id. Best created using
 * {@link org.elasticsearch.client.Requests#getRequest(String)}.
 * <p>
 * The operation requires the {@link #index()}, {@link #type(String)} and {@link #id(String)}
 * to be set.
 *
 * @see org.elasticsearch.action.get.GetResponse
 * @see org.elasticsearch.client.Requests#getRequest(String)
 * @see org.elasticsearch.client.Client#get(GetRequest)
 */
public class GetRequest extends SingleShardRequest<GetRequest> implements RealtimeRequest {

    private String type;
    private String id;
    private String routing;
    private String parent;
    private String preference;

    private String[] fields;

    private FetchSourceContext fetchSourceContext;

    private boolean refresh = false;

    boolean realtime = true;

    private VersionType versionType = VersionType.INTERNAL;
    private long version = Versions.MATCH_ANY;
    private boolean ignoreErrorsOnGeneratedFields;

    public GetRequest() {
        type = "_all";
    }

    /**
     * Constructs a new get request against the specified index. The {@link #type(String)} and {@link #id(String)}
     * must be set.
     */
    public GetRequest(String index) {
        super(index);
        this.type = "_all";
    }

    /**
     * Constructs a new get request against the specified index with the type and id.
     *
     * @param index The index to get the document from
     * @param type  The type of the document
     * @param id    The id of the document
     */
    public GetRequest(String index, String type, String id) {
        super(index);
        this.type = type;
        this.id = id;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validateNonNullIndex();
        if (type == null) {
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

    /**
     * Sets the type of the document to fetch.
     */
    public GetRequest type(@Nullable String type) {
        if (type == null) {
            type = "_all";
        }
        this.type = type;
        return this;
    }

    /**
     * Sets the id of the document to fetch.
     */
    public GetRequest id(String id) {
        this.id = id;
        return this;
    }

    /**
     * @return The parent for this request.
     */
    public String parent() {
        return parent;
    }

    /**
     * Sets the parent id of this document.
     */
    public GetRequest parent(String parent) {
        this.parent = parent;
        return this;
    }

    /**
     * Controls the shard routing of the request. Using this value to hash the shard
     * and not the id.
     */
    public GetRequest routing(String routing) {
        this.routing = routing;
        return this;
    }

    /**
     * Sets the preference to execute the search. Defaults to randomize across shards. Can be set to
     * <tt>_local</tt> to prefer local shards, <tt>_primary</tt> to execute only on primary shards, or
     * a custom value, which guarantees that the same order will be used across different requests.
     */
    public GetRequest preference(String preference) {
        this.preference = preference;
        return this;
    }

    public String type() {
        return type;
    }

    public String id() {
        return id;
    }

    public String routing() {
        return this.routing;
    }

    public String preference() {
        return this.preference;
    }

    /**
     * Allows setting the {@link FetchSourceContext} for this request, controlling if and how _source should be returned.
     */
    public GetRequest fetchSourceContext(FetchSourceContext context) {
        this.fetchSourceContext = context;
        return this;
    }

    public FetchSourceContext fetchSourceContext() {
        return fetchSourceContext;
    }

    /**
     * Explicitly specify the fields that will be returned. By default, the <tt>_source</tt>
     * field will be returned.
     */
    public GetRequest fields(String... fields) {
        this.fields = fields;
        return this;
    }

    /**
     * Explicitly specify the fields that will be returned. By default, the <tt>_source</tt>
     * field will be returned.
     */
    public String[] fields() {
        return this.fields;
    }

    /**
     * Should a refresh be executed before this get operation causing the operation to
     * return the latest value. Note, heavy get should not set this to <tt>true</tt>. Defaults
     * to <tt>false</tt>.
     */
    public GetRequest refresh(boolean refresh) {
        this.refresh = refresh;
        return this;
    }

    public boolean refresh() {
        return this.refresh;
    }

    public boolean realtime() {
        return this.realtime;
    }

    @Override
    public GetRequest realtime(boolean realtime) {
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

    public GetRequest version(long version) {
        this.version = version;
        return this;
    }

    /**
     * Sets the versioning type. Defaults to {@link org.elasticsearch.index.VersionType#INTERNAL}.
     */
    public GetRequest versionType(VersionType versionType) {
        this.versionType = versionType;
        return this;
    }

    public GetRequest ignoreErrorsOnGeneratedFields(boolean ignoreErrorsOnGeneratedFields) {
        this.ignoreErrorsOnGeneratedFields = ignoreErrorsOnGeneratedFields;
        return this;
    }

    public VersionType versionType() {
        return this.versionType;
    }

    public boolean ignoreErrorsOnGeneratedFields() {
        return ignoreErrorsOnGeneratedFields;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        type = in.readString();
        id = in.readString();
        routing = in.readOptionalString();
        parent = in.readOptionalString();
        preference = in.readOptionalString();
        refresh = in.readBoolean();
        int size = in.readInt();
        if (size >= 0) {
            fields = new String[size];
            for (int i = 0; i < size; i++) {
                fields[i] = in.readString();
            }
        }
        realtime = in.readBoolean();
        this.ignoreErrorsOnGeneratedFields = in.readBoolean();

        this.versionType = VersionType.fromValue(in.readByte());
        this.version = in.readLong();
        fetchSourceContext = in.readOptionalStreamable(FetchSourceContext::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(type);
        out.writeString(id);
        out.writeOptionalString(routing);
        out.writeOptionalString(parent);
        out.writeOptionalString(preference);

        out.writeBoolean(refresh);
        if (fields == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(fields.length);
            for (String field : fields) {
                out.writeString(field);
            }
        }
        out.writeBoolean(realtime);
        out.writeBoolean(ignoreErrorsOnGeneratedFields);
        out.writeByte(versionType.getValue());
        out.writeLong(version);
        out.writeOptionalStreamable(fetchSourceContext);
    }

    @Override
    public String toString() {
        return "get [" + index + "][" + type + "][" + id + "]: routing [" + routing + "]";
    }

}
