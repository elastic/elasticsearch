/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.get;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.RealtimeRequest;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A request to get a document (its source) from an index based on its id. Best created using
 * {@link org.elasticsearch.client.Requests#getRequest(String)}.
 * <p>
 * The operation requires the {@link #index()}, {@link #type(String)} and {@link #id(String)}
 * to be set.
 *
 * @see org.elasticsearch.action.get.GetResponse
 * @see org.elasticsearch.client.Requests#getRequest(String)
 * @see org.elasticsearch.client.Client#get(GetRequest)
 */
// It's not possible to suppress teh warning at #realtime(boolean) at a method-level.
@SuppressWarnings("unchecked")
public class GetRequest extends SingleShardRequest<GetRequest> implements RealtimeRequest {

    private String type;
    private String id;
    private String routing;
    private String preference;

    private String[] storedFields;

    private FetchSourceContext fetchSourceContext;

    private boolean refresh = false;

    boolean realtime = true;

    private VersionType versionType = VersionType.INTERNAL;
    private long version = Versions.MATCH_ANY;

    public GetRequest() {
        type = MapperService.SINGLE_MAPPING_NAME;
    }

    GetRequest(StreamInput in) throws IOException {
        super(in);
        type = in.readString();
        id = in.readString();
        routing = in.readOptionalString();
        if (in.getVersion().before(Version.V_7_0_0)) {
            in.readOptionalString();
        }
        preference = in.readOptionalString();
        refresh = in.readBoolean();
        storedFields = in.readOptionalStringArray();
        realtime = in.readBoolean();

        this.versionType = VersionType.fromValue(in.readByte());
        this.version = in.readLong();
        fetchSourceContext = in.readOptionalWriteable(FetchSourceContext::new);
    }

    /**
     * Constructs a new get request against the specified index. The {@link #id(String)} must also be set.
     */
    public GetRequest(String index) {
        super(index);
        this.type = MapperService.SINGLE_MAPPING_NAME;
    }

    /**
     * Constructs a new get request against the specified index with the type and id.
     *
     * @param index The index to get the document from
     * @param type  The type of the document
     * @param id    The id of the document
     * @deprecated Types are in the process of being removed, use {@link GetRequest(String, String)} instead.
     */
    @Deprecated
    public GetRequest(String index, String type, String id) {
        super(index);
        this.type = type;
        this.id = id;
    }

    /**
     * Constructs a new get request against the specified index and document ID.
     *
     * @param index The index to get the document from
     * @param id    The id of the document
     */
    public GetRequest(String index, String id) {
        super(index);
        this.id = id;
        this.type = MapperService.SINGLE_MAPPING_NAME;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validateNonNullIndex();
        if (Strings.isEmpty(type)) {
            validationException = addValidationError("type is missing", validationException);
        }
        if (Strings.isEmpty(id)) {
            validationException = addValidationError("id is missing", validationException);
        }
        if (versionType.validateVersionForReads(version) == false) {
            validationException = ValidateActions.addValidationError("illegal version value [" + version + "] for version type ["
                    + versionType.name() + "]", validationException);
        }
        return validationException;
    }

    /**
     * Sets the type of the document to fetch.
     * @deprecated Types are in the process of being removed.
     */
    @Deprecated
    public GetRequest type(@Nullable String type) {
        if (type == null) {
            type = MapperService.SINGLE_MAPPING_NAME;
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
     * Controls the shard routing of the request. Using this value to hash the shard
     * and not the id.
     */
    public GetRequest routing(String routing) {
        this.routing = routing;
        return this;
    }

    /**
     * Sets the preference to execute the search. Defaults to randomize across shards. Can be set to
     * {@code _local} to prefer local shards or a custom value, which guarantees that the same order
     * will be used across different requests.
     */
    public GetRequest preference(String preference) {
        this.preference = preference;
        return this;
    }

    /**
     * @deprecated Types are in the process of being removed.
     */
    @Deprecated
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
     * Explicitly specify the stored fields that will be returned. By default, the {@code _source}
     * field will be returned.
     */
    public GetRequest storedFields(String... fields) {
        this.storedFields = fields;
        return this;
    }

    /**
     * Explicitly specify the stored fields that will be returned. By default, the {@code _source}
     * field will be returned.
     */
    public String[] storedFields() {
        return this.storedFields;
    }

    /**
     * Should a refresh be executed before this get operation causing the operation to
     * return the latest value. Note, heavy get should not set this to {@code true}. Defaults
     * to {@code false}.
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

    public VersionType versionType() {
        return this.versionType;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(type);
        out.writeString(id);
        out.writeOptionalString(routing);
        if (out.getVersion().before(Version.V_7_0_0)) {
            out.writeOptionalString(null);
        }
        out.writeOptionalString(preference);

        out.writeBoolean(refresh);
        out.writeOptionalStringArray(storedFields);
        out.writeBoolean(realtime);
        out.writeByte(versionType.getValue());
        out.writeLong(version);
        out.writeOptionalWriteable(fetchSourceContext);
    }

    @Override
    public String toString() {
        return "get [" + index + "][" + type + "][" + id + "]: routing [" + routing + "]";
    }

}
