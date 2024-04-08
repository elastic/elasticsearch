/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.get;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.RealtimeRequest;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A request to get a document (its source) from an index based on its id.
 * <p>
 * The operation requires the {@link #index()} and {@link #id(String)}
 * to be set.
 *
 * @see org.elasticsearch.action.get.GetResponse
 * @see org.elasticsearch.client.internal.Client#get(GetRequest)
 */
// It's not possible to suppress teh warning at #realtime(boolean) at a method-level.
@SuppressWarnings("unchecked")
public class GetRequest extends SingleShardRequest<GetRequest> implements RealtimeRequest {

    private String id;
    private String routing;
    private String preference;

    private String[] storedFields;

    private FetchSourceContext fetchSourceContext;

    private boolean refresh = false;

    boolean realtime = true;

    private VersionType versionType = VersionType.INTERNAL;
    private long version = Versions.MATCH_ANY;

    /**
     * Should this request force {@link SourceLoader.Synthetic synthetic source}?
     * Use this to test if the mapping supports synthetic _source and to get a sense
     * of the worst case performance. Fetches with this enabled will be slower the
     * enabling synthetic source natively in the index.
     */
    private boolean forceSyntheticSource = false;

    public GetRequest() {}

    public GetRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getTransportVersion().before(TransportVersions.V_8_0_0)) {
            in.readString();
        }
        id = in.readString();
        routing = in.readOptionalString();
        preference = in.readOptionalString();
        refresh = in.readBoolean();
        storedFields = in.readOptionalStringArray();
        realtime = in.readBoolean();

        this.versionType = VersionType.fromValue(in.readByte());
        this.version = in.readLong();
        fetchSourceContext = in.readOptionalWriteable(FetchSourceContext::readFrom);
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_4_0)) {
            forceSyntheticSource = in.readBoolean();
        } else {
            forceSyntheticSource = false;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getTransportVersion().before(TransportVersions.V_8_0_0)) {
            out.writeString(MapperService.SINGLE_MAPPING_NAME);
        }
        out.writeString(id);
        out.writeOptionalString(routing);
        out.writeOptionalString(preference);

        out.writeBoolean(refresh);
        out.writeOptionalStringArray(storedFields);
        out.writeBoolean(realtime);
        out.writeByte(versionType.getValue());
        out.writeLong(version);
        out.writeOptionalWriteable(fetchSourceContext);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_4_0)) {
            out.writeBoolean(forceSyntheticSource);
        } else {
            if (forceSyntheticSource) {
                throw new IllegalArgumentException("force_synthetic_source is not supported before 8.4.0");
            }
        }
    }

    /**
     * Constructs a new get request against the specified index. The {@link #id(String)} must also be set.
     */
    public GetRequest(String index) {
        super(index);
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
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validateNonNullIndex();
        if (Strings.isEmpty(id)) {
            validationException = addValidationError("id is missing", validationException);
        }
        if (versionType.validateVersionForReads(version) == false) {
            validationException = ValidateActions.addValidationError(
                "illegal version value [" + version + "] for version type [" + versionType.name() + "]",
                validationException
            );
        }
        return validationException;
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

    /**
     * Should this request force {@link SourceLoader.Synthetic synthetic source}?
     * Use this to test if the mapping supports synthetic _source and to get a sense
     * of the worst case performance. Fetches with this enabled will be slower the
     * enabling synthetic source natively in the index.
     */
    public void setForceSyntheticSource(boolean forceSyntheticSource) {
        this.forceSyntheticSource = forceSyntheticSource;
    }

    /**
     * Should this request force {@link SourceLoader.Synthetic synthetic source}?
     * Use this to test if the mapping supports synthetic _source and to get a sense
     * of the worst case performance. Fetches with this enabled will be slower the
     * enabling synthetic source natively in the index.
     */
    public boolean isForceSyntheticSource() {
        return forceSyntheticSource;
    }

    @Override
    public String toString() {
        return "get [" + index + "][" + id + "]: routing [" + routing + "]";
    }

}
