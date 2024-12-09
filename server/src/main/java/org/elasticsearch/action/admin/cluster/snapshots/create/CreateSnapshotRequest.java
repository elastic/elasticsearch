/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.snapshots.create;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.Strings.EMPTY_ARRAY;
import static org.elasticsearch.common.settings.Settings.readSettingsFromStream;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;

/**
 * Create snapshot request
 * <p>
 * The only mandatory parameter is repository name. The repository name has to satisfy the following requirements
 * <ul>
 * <li>be a non-empty string</li>
 * <li>must not contain whitespace (tabs or spaces)</li>
 * <li>must not contain comma (',')</li>
 * <li>must not contain hash sign ('#')</li>
 * <li>must not start with underscore ('-')</li>
 * <li>must be lowercase</li>
 * <li>must not contain invalid file name characters {@link org.elasticsearch.common.Strings#INVALID_FILENAME_CHARS} </li>
 * </ul>
 */
public class CreateSnapshotRequest extends MasterNodeRequest<CreateSnapshotRequest>
    implements
        IndicesRequest.Replaceable,
        ToXContentObject {

    public static final TransportVersion SETTINGS_IN_REQUEST_VERSION = TransportVersions.V_8_0_0;

    public static int MAXIMUM_METADATA_BYTES = 1024; // chosen arbitrarily

    private String snapshot;

    private String repository;

    private String[] indices = EMPTY_ARRAY;

    private IndicesOptions indicesOptions = DataStream.isFailureStoreFeatureFlagEnabled()
        ? IndicesOptions.strictExpandHiddenIncludeFailureStore()
        : IndicesOptions.strictExpandHidden();

    private String[] featureStates = EMPTY_ARRAY;

    private boolean partial = false;

    private boolean includeGlobalState = true;

    private boolean waitForCompletion;

    @Nullable
    private Map<String, Object> userMetadata;

    @Nullable
    private String uuid = null;

    public CreateSnapshotRequest(TimeValue masterNodeTimeout) {
        super(masterNodeTimeout);
    }

    /**
     * Constructs a new put repository request with the provided snapshot and repository names
     *
     * @param repository repository name
     * @param snapshot   snapshot name
     */
    public CreateSnapshotRequest(TimeValue masterNodeTimeout, String repository, String snapshot) {
        this(masterNodeTimeout);
        this.snapshot = snapshot;
        this.repository = repository;
        this.uuid = UUIDs.randomBase64UUID();
    }

    public CreateSnapshotRequest(StreamInput in) throws IOException {
        super(in);
        snapshot = in.readString();
        repository = in.readString();
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        if (in.getTransportVersion().before(SETTINGS_IN_REQUEST_VERSION)) {
            readSettingsFromStream(in);
        }
        featureStates = in.readStringArray();
        includeGlobalState = in.readBoolean();
        waitForCompletion = in.readBoolean();
        partial = in.readBoolean();
        userMetadata = in.readGenericMap();
        uuid = in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0) ? in.readOptionalString() : null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(snapshot);
        out.writeString(repository);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
        if (out.getTransportVersion().before(SETTINGS_IN_REQUEST_VERSION)) {
            Settings.EMPTY.writeTo(out);
        }
        out.writeStringArray(featureStates);
        out.writeBoolean(includeGlobalState);
        out.writeBoolean(waitForCompletion);
        out.writeBoolean(partial);
        out.writeGenericMap(userMetadata);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            out.writeOptionalString(uuid);
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (snapshot == null) {
            validationException = addValidationError("snapshot is missing", validationException);
        }
        if (repository == null) {
            validationException = addValidationError("repository is missing", validationException);
        }
        if (indices == null) {
            validationException = addValidationError("indices is null", validationException);
        } else {
            for (String index : indices) {
                if (index == null) {
                    validationException = addValidationError("index is null", validationException);
                    break;
                }
            }
        }
        if (indicesOptions == null) {
            validationException = addValidationError("indicesOptions is null", validationException);
        }
        if (featureStates == null) {
            validationException = addValidationError("featureStates is null", validationException);
        }
        final int metadataSize = metadataSize(userMetadata);
        if (metadataSize > MAXIMUM_METADATA_BYTES) {
            validationException = addValidationError(
                "metadata must be smaller than 1024 bytes, but was [" + metadataSize + "]",
                validationException
            );
        }
        return validationException;
    }

    public static int metadataSize(Map<String, Object> userMetadata) {
        if (userMetadata == null) {
            return 0;
        }
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.value(userMetadata);
            int size = BytesReference.bytes(builder).length();
            return size;
        } catch (IOException e) {
            // This should not be possible as we are just rendering the xcontent in memory
            throw new ElasticsearchException(e);
        }
    }

    /**
     * Sets the snapshot name
     *
     * @param snapshot snapshot name
     */
    public CreateSnapshotRequest snapshot(String snapshot) {
        this.snapshot = snapshot;
        return this;
    }

    /**
     * The snapshot name
     *
     * @return snapshot name
     */
    public String snapshot() {
        return this.snapshot;
    }

    /**
     * Sets repository name
     *
     * @param repository name
     * @return this request
     */
    public CreateSnapshotRequest repository(String repository) {
        this.repository = repository;
        return this;
    }

    /**
     * Returns repository name
     *
     * @return repository name
     */
    public String repository() {
        return this.repository;
    }

    /**
     * Sets a list of indices that should be included into the snapshot
     * <p>
     * The list of indices supports multi-index syntax. For example: "+test*" ,"-test42" will index all indices with
     * prefix "test" except index "test42". Aliases are supported. An empty list or {"_all"} will snapshot all open
     * indices in the cluster.
     *
     * @return this request
     */
    @Override
    public CreateSnapshotRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    /**
     * Sets a list of indices that should be included into the snapshot
     * <p>
     * The list of indices supports multi-index syntax. For example: "+test*" ,"-test42" will index all indices with
     * prefix "test" except index "test42". Aliases are supported. An empty list or {"_all"} will snapshot all open
     * indices in the cluster.
     *
     * @return this request
     */
    public CreateSnapshotRequest indices(List<String> indices) {
        this.indices = indices.toArray(new String[indices.size()]);
        return this;
    }

    /**
     * Returns a list of indices that should be included into the snapshot
     *
     * @return list of indices
     */
    @Override
    public String[] indices() {
        return indices;
    }

    /**
     * Specifies the indices options. Like what type of requested indices to ignore. For example indices that don't exist.
     *
     * @return the desired behaviour regarding indices options
     */
    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    /**
     * Specifies the indices options. Like what type of requested indices to ignore. For example indices that don't exist.
     *
     * @param indicesOptions the desired behaviour regarding indices options
     * @return this request
     */
    public CreateSnapshotRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    /**
     * Returns true if indices with unavailable shards should be be partially snapshotted.
     *
     * @return the desired behaviour regarding indices options
     */
    public boolean partial() {
        return partial;
    }

    /**
     * Set to true to allow indices with unavailable shards to be partially snapshotted.
     *
     * @param partial true if indices with unavailable shards should be be partially snapshotted.
     * @return this request
     */
    public CreateSnapshotRequest partial(boolean partial) {
        this.partial = partial;
        return this;
    }

    /**
     * If set to true the operation should wait for the snapshot completion before returning.
     *
     * By default, the operation will return as soon as snapshot is initialized. It can be changed by setting this
     * flag to true.
     *
     * @param waitForCompletion true if operation should wait for the snapshot completion
     * @return this request
     */
    public CreateSnapshotRequest waitForCompletion(boolean waitForCompletion) {
        this.waitForCompletion = waitForCompletion;
        return this;
    }

    /**
     * Returns true if the request should wait for the snapshot completion before returning
     *
     * @return true if the request should wait for completion
     */
    public boolean waitForCompletion() {
        return waitForCompletion;
    }

    /**
     * Set to true if global state should be stored as part of the snapshot
     *
     * @param includeGlobalState true if global state should be stored
     * @return this request
     */
    public CreateSnapshotRequest includeGlobalState(boolean includeGlobalState) {
        this.includeGlobalState = includeGlobalState;
        return this;
    }

    /**
     * Returns true if global state should be stored as part of the snapshot
     *
     * @return true if global state should be stored as part of the snapshot
     */
    public boolean includeGlobalState() {
        return includeGlobalState;
    }

    /**
     * @return user metadata map that should be stored with the snapshot or {@code null} if there is no user metadata to be associated with
     *         this snapshot
     */
    @Nullable
    public Map<String, Object> userMetadata() {
        return userMetadata;
    }

    /**
     * @param userMetadata user metadata map that should be stored with the snapshot
     */
    public CreateSnapshotRequest userMetadata(@Nullable Map<String, Object> userMetadata) {
        this.userMetadata = userMetadata;
        return this;
    }

    /**
     * Set a uuid to identify snapshot.
     * If no uuid is specified, one will be created within SnapshotService
     */
    public CreateSnapshotRequest uuid(String uuid) {
        this.uuid = uuid;
        return this;
    }

    /**
     * Get the uuid, generating it if one does not yet exist.
     * Because the uuid can be set, this method is NOT thread-safe.
     * <p>
     * The uuid was previously generated in SnapshotService.createSnapshot
     * but was moved to the CreateSnapshotRequest constructor so that the caller could
     * uniquely identify the snapshot. Unfortunately, in a mixed-version cluster,
     * the CreateSnapshotRequest could be created on a node which does not yet
     * generate the uuid in the constructor. In this case, the uuid
     * must be generated when it is first accessed with this getter.
     *
     * @return the uuid that will be used for the snapshot
     */
    public String uuid() {
        if (this.uuid == null) {
            this.uuid = UUIDs.randomBase64UUID();
        }
        return this.uuid;
    }

    /**
     * @return Which plugin states should be included in the snapshot
     */
    public String[] featureStates() {
        return featureStates;
    }

    /**
     * @param featureStates The plugin states to be included in the snapshot
     */
    public CreateSnapshotRequest featureStates(String[] featureStates) {
        this.featureStates = featureStates;
        return this;
    }

    /**
     * @param featureStates The plugin states to be included in the snapshot
     */
    public CreateSnapshotRequest featureStates(List<String> featureStates) {
        return featureStates(featureStates.toArray(EMPTY_ARRAY));
    }

    /**
     * Parses snapshot definition.
     *
     * @param source snapshot definition
     * @return this request
     */
    @SuppressWarnings("unchecked")
    public CreateSnapshotRequest source(Map<String, Object> source) {
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            String name = entry.getKey();
            switch (name) {
                case "indices":
                    if (entry.getValue() instanceof String) {
                        indices(Strings.splitStringByCommaToArray((String) entry.getValue()));
                    } else if (entry.getValue() instanceof List) {
                        indices((List<String>) entry.getValue());
                    } else {
                        throw new IllegalArgumentException("malformed indices section, should be an array of strings");
                    }
                    break;
                case "feature_states":
                    if (entry.getValue() instanceof List) {
                        featureStates((List<String>) entry.getValue());
                    } else {
                        throw new IllegalArgumentException("malformed feature_states section, should be an array of strings");
                    }
                    break;
                case "partial":
                    partial(nodeBooleanValue(entry.getValue(), "partial"));
                    break;
                case "include_global_state":
                    includeGlobalState = nodeBooleanValue(entry.getValue(), "include_global_state");
                    break;
                case "metadata":
                    if (entry.getValue() != null && (entry.getValue() instanceof Map == false)) {
                        throw new IllegalArgumentException("malformed metadata, should be an object");
                    }
                    userMetadata((Map<String, Object>) entry.getValue());
                    break;
            }
        }
        indicesOptions(IndicesOptions.fromMap(source, indicesOptions));
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("repository", repository);
        builder.field("snapshot", snapshot);
        builder.array("indices", indices);
        if (featureStates != null) {
            builder.array("feature_states", featureStates);
        }
        builder.field("partial", partial);
        builder.field("include_global_state", includeGlobalState);
        if (indicesOptions != null) {
            indicesOptions.toXContent(builder, params);
        }
        builder.field("metadata", userMetadata);
        builder.endObject();
        return builder;
    }

    @Override
    public String getDescription() {
        return "snapshot [" + repository + ":" + snapshot + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreateSnapshotRequest that = (CreateSnapshotRequest) o;
        return partial == that.partial
            && includeGlobalState == that.includeGlobalState
            && waitForCompletion == that.waitForCompletion
            && Objects.equals(snapshot, that.snapshot)
            && Objects.equals(repository, that.repository)
            && Arrays.equals(indices, that.indices)
            && Objects.equals(indicesOptions, that.indicesOptions)
            && Arrays.equals(featureStates, that.featureStates)
            && Objects.equals(masterNodeTimeout(), that.masterNodeTimeout())
            && Objects.equals(userMetadata, that.userMetadata)
            && Objects.equals(uuid, that.uuid);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(snapshot, repository, indicesOptions, partial, includeGlobalState, waitForCompletion, userMetadata, uuid);
        result = 31 * result + Arrays.hashCode(indices);
        result = 31 * result + Arrays.hashCode(featureStates);
        return result;
    }

    @Override
    public String toString() {
        return "CreateSnapshotRequest{"
            + "snapshot='"
            + snapshot
            + '\''
            + ", repository='"
            + repository
            + '\''
            + ", indices="
            + (indices == null ? null : Arrays.asList(indices))
            + ", indicesOptions="
            + indicesOptions
            + ", featureStates="
            + Arrays.asList(featureStates)
            + ", partial="
            + partial
            + ", includeGlobalState="
            + includeGlobalState
            + ", waitForCompletion="
            + waitForCompletion
            + ", masterNodeTimeout="
            + masterNodeTimeout()
            + ", metadata="
            + userMetadata
            + ", uuid="
            + uuid
            + '}';
    }
}
