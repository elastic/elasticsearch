/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.restore;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.settings.Settings.readSettingsFromStream;
import static org.elasticsearch.common.settings.Settings.writeSettingsToStream;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;

/**
 * Restore snapshot request
 */
public class RestoreSnapshotRequest extends MasterNodeRequest<RestoreSnapshotRequest> implements ToXContentObject {

    private String snapshot;
    private String repository;
    private String[] indices = Strings.EMPTY_ARRAY;
    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpen();
    private String[] featureStates = Strings.EMPTY_ARRAY;
    private String renamePattern;
    private String renameReplacement;
    private boolean waitForCompletion;
    private boolean includeGlobalState = false;
    private boolean partial = false;
    private boolean includeAliases = true;
    private Settings indexSettings = Settings.EMPTY;
    private String[] ignoreIndexSettings = Strings.EMPTY_ARRAY;

    // This field does not get serialised (except toString for debugging purpose) because it is always set locally by authz
    private boolean skipOperatorOnlyState = false;

    @Nullable // if any snapshot UUID will do
    private String snapshotUuid;

    public RestoreSnapshotRequest() {}

    /**
     * Constructs a new put repository request with the provided repository and snapshot names.
     *
     * @param repository repository name
     * @param snapshot   snapshot name
     */
    public RestoreSnapshotRequest(String repository, String snapshot) {
        this.snapshot = snapshot;
        this.repository = repository;
    }

    public RestoreSnapshotRequest(StreamInput in) throws IOException {
        super(in);
        snapshot = in.readString();
        repository = in.readString();
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        featureStates = in.readStringArray();
        renamePattern = in.readOptionalString();
        renameReplacement = in.readOptionalString();
        waitForCompletion = in.readBoolean();
        includeGlobalState = in.readBoolean();
        partial = in.readBoolean();
        includeAliases = in.readBoolean();
        indexSettings = readSettingsFromStream(in);
        ignoreIndexSettings = in.readStringArray();
        snapshotUuid = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(snapshot);
        out.writeString(repository);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
        out.writeStringArray(featureStates);
        out.writeOptionalString(renamePattern);
        out.writeOptionalString(renameReplacement);
        out.writeBoolean(waitForCompletion);
        out.writeBoolean(includeGlobalState);
        out.writeBoolean(partial);
        out.writeBoolean(includeAliases);
        writeSettingsToStream(indexSettings, out);
        out.writeStringArray(ignoreIndexSettings);
        out.writeOptionalString(snapshotUuid);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (snapshot == null) {
            validationException = addValidationError("name is missing", validationException);
        }
        if (repository == null) {
            validationException = addValidationError("repository is missing", validationException);
        }
        if (indices == null) {
            validationException = addValidationError("indices are missing", validationException);
        }
        if (indicesOptions == null) {
            validationException = addValidationError("indicesOptions is missing", validationException);
        }
        if (featureStates == null) {
            validationException = addValidationError("featureStates is missing", validationException);
        }
        if (indexSettings == null) {
            validationException = addValidationError("indexSettings are missing", validationException);
        }
        if (ignoreIndexSettings == null) {
            validationException = addValidationError("ignoreIndexSettings are missing", validationException);
        }
        return validationException;
    }

    /**
     * Sets the name of the snapshot.
     *
     * @param snapshot snapshot name
     * @return this request
     */
    public RestoreSnapshotRequest snapshot(String snapshot) {
        this.snapshot = snapshot;
        return this;
    }

    /**
     * Returns the name of the snapshot.
     *
     * @return snapshot name
     */
    public String snapshot() {
        return this.snapshot;
    }

    /**
     * Sets repository name
     *
     * @param repository repository name
     * @return this request
     */
    public RestoreSnapshotRequest repository(String repository) {
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
     * Sets the list of indices that should be restored from snapshot
     * <p>
     * The list of indices supports multi-index syntax. For example: "+test*" ,"-test42" will index all indices with
     * prefix "test" except index "test42". Aliases are not supported. An empty list or {"_all"} will restore all open
     * indices in the snapshot.
     *
     * @param indices list of indices
     * @return this request
     */
    public RestoreSnapshotRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    /**
     * Sets the list of indices that should be restored from snapshot
     * <p>
     * The list of indices supports multi-index syntax. For example: "+test*" ,"-test42" will index all indices with
     * prefix "test" except index "test42". Aliases are not supported. An empty list or {"_all"} will restore all open
     * indices in the snapshot.
     *
     * @param indices list of indices
     * @return this request
     */
    public RestoreSnapshotRequest indices(List<String> indices) {
        this.indices = indices.toArray(new String[indices.size()]);
        return this;
    }

    /**
     * Returns list of indices that should be restored from snapshot
     */
    public String[] indices() {
        return indices;
    }

    /**
     * Specifies what type of requested indices to ignore and how to deal with wildcard expressions.
     * For example indices that don't exist.
     *
     * @return the desired behaviour regarding indices to ignore and wildcard indices expression
     */
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    /**
     * Specifies what type of requested indices to ignore and how to deal with wildcard expressions.
     * For example indices that don't exist.
     *
     * @param indicesOptions the desired behaviour regarding indices to ignore and wildcard indices expressions
     * @return this request
     */
    public RestoreSnapshotRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    /**
     * Sets rename pattern that should be applied to restored indices.
     * <p>
     * Indices that match the rename pattern will be renamed according to {@link #renameReplacement(String)}. The
     * rename pattern is applied according to the {@link java.util.regex.Matcher#appendReplacement(StringBuffer, String)}
     * The request will fail if two or more indices will be renamed into the same name.
     *
     * @param renamePattern rename pattern
     * @return this request
     */
    public RestoreSnapshotRequest renamePattern(String renamePattern) {
        this.renamePattern = renamePattern;
        return this;
    }

    /**
     * Returns rename pattern
     *
     * @return rename pattern
     */
    public String renamePattern() {
        return renamePattern;
    }

    /**
     * Sets rename replacement
     * <p>
     * See {@link #renamePattern(String)} for more information.
     *
     * @param renameReplacement rename replacement
     */
    public RestoreSnapshotRequest renameReplacement(String renameReplacement) {
        this.renameReplacement = renameReplacement;
        return this;
    }

    /**
     * Returns rename replacement
     *
     * @return rename replacement
     */
    public String renameReplacement() {
        return renameReplacement;
    }

    /**
     * If this parameter is set to true the operation will wait for completion of restore process before returning.
     *
     * @param waitForCompletion if true the operation will wait for completion
     * @return this request
     */
    public RestoreSnapshotRequest waitForCompletion(boolean waitForCompletion) {
        this.waitForCompletion = waitForCompletion;
        return this;
    }

    /**
     * Returns wait for completion setting
     *
     * @return true if the operation will wait for completion
     */
    public boolean waitForCompletion() {
        return waitForCompletion;
    }

    /**
     * Returns true if indices with failed to snapshot shards should be partially restored.
     *
     * @return true if indices with failed to snapshot shards should be partially restored
     */
    public boolean partial() {
        return partial;
    }

    /**
     * Set to true to allow indices with failed to snapshot shards should be partially restored.
     *
     * @param partial true if indices with failed to snapshot shards should be partially restored.
     * @return this request
     */
    public RestoreSnapshotRequest partial(boolean partial) {
        this.partial = partial;
        return this;
    }

    /**
     * Sets the list of index settings and index settings groups that shouldn't be restored from snapshot
     */
    public RestoreSnapshotRequest ignoreIndexSettings(String... ignoreIndexSettings) {
        this.ignoreIndexSettings = ignoreIndexSettings;
        return this;
    }

    /**
     * Sets the list of index settings and index settings groups that shouldn't be restored from snapshot
     */
    public RestoreSnapshotRequest ignoreIndexSettings(List<String> ignoreIndexSettings) {
        this.ignoreIndexSettings = ignoreIndexSettings.toArray(new String[ignoreIndexSettings.size()]);
        return this;
    }

    /**
     * Returns the list of index settings and index settings groups that shouldn't be restored from snapshot
     */
    public String[] ignoreIndexSettings() {
        return ignoreIndexSettings;
    }

    /**
     * If set to true the restore procedure will restore global cluster state.
     * <p>
     * The global cluster state includes persistent settings and index template definitions.
     *
     * @param includeGlobalState true if global state should be restored from the snapshot
     * @return this request
     */
    public RestoreSnapshotRequest includeGlobalState(boolean includeGlobalState) {
        this.includeGlobalState = includeGlobalState;
        return this;
    }

    /**
     * Returns true if global state should be restored from this snapshot
     *
     * @return true if global state should be restored
     */
    public boolean includeGlobalState() {
        return includeGlobalState;
    }

    /**
     * If set to true the restore procedure will restore aliases
     *
     * @param includeAliases true if aliases should be restored from the snapshot
     * @return this request
     */
    public RestoreSnapshotRequest includeAliases(boolean includeAliases) {
        this.includeAliases = includeAliases;
        return this;
    }

    /**
     * Returns true if aliases should be restored from this snapshot
     *
     * @return true if aliases should be restored
     */
    public boolean includeAliases() {
        return includeAliases;
    }

    /**
     * Sets settings that should be added/changed in all restored indices
     */
    public RestoreSnapshotRequest indexSettings(Settings settings) {
        this.indexSettings = settings;
        return this;
    }

    /**
     * Sets settings that should be added/changed in all restored indices
     */
    public RestoreSnapshotRequest indexSettings(Settings.Builder settings) {
        this.indexSettings = settings.build();
        return this;
    }

    /**
     * Sets settings that should be added/changed in all restored indices
     */
    public RestoreSnapshotRequest indexSettings(String source, XContentType xContentType) {
        this.indexSettings = Settings.builder().loadFromSource(source, xContentType).build();
        return this;
    }

    /**
     * Sets settings that should be added/changed in all restored indices
     */
    public RestoreSnapshotRequest indexSettings(Map<String, Object> source) {
        this.indexSettings = Settings.builder().loadFromMap(source).build();
        return this;
    }

    /**
     * Returns settings that should be added/changed in all restored indices
     */
    public Settings indexSettings() {
        return this.indexSettings;
    }

    /**
     * Sometimes a client has identified precisely which snapshot is to be restored via a separate mechanism and wishes to guarantee that
     * this is the snapshot that this request restores. If the client can only identify a snapshot by its name then there is a risk that the
     * desired snapshot may be deleted and replaced by a new snapshot with the same name which is inconsistent with the original one. This
     * method lets us fail the restore if the precise snapshot we want is not available.
     *
     * This is for internal use only and is not exposed in the REST layer.
     */
    public RestoreSnapshotRequest snapshotUuid(String snapshotUuid) {
        this.snapshotUuid = snapshotUuid;
        return this;
    }

    /**
     * @return the UUID that identifies the specific snapshot in the repository to be restored, or {@code null} if the snapshot name is
     * a sufficient identifier.
     */
    @Nullable
    public String snapshotUuid() {
        return snapshotUuid;
    }

    public boolean skipOperatorOnlyState() {
        return skipOperatorOnlyState;
    }

    public void skipOperatorOnlyState(boolean skipOperatorOnlyState) {
        this.skipOperatorOnlyState = skipOperatorOnlyState;
    }

    /**
     * @return Which feature states should be included in the snapshot
     */
    @Nullable
    public String[] featureStates() {
        return featureStates;
    }

    /**
     * @param featureStates The feature states to be included in the snapshot
     */
    public RestoreSnapshotRequest featureStates(String[] featureStates) {
        this.featureStates = featureStates;
        return this;
    }

    /**
     * @param featureStates The feature states to be included in the snapshot
     */
    public RestoreSnapshotRequest featureStates(List<String> featureStates) {
        return featureStates(featureStates.toArray(Strings.EMPTY_ARRAY));
    }

    /**
     * Parses restore definition
     *
     * @param source restore definition
     * @return this request
     */
    @SuppressWarnings("unchecked")
    public RestoreSnapshotRequest source(Map<String, Object> source) {
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            String name = entry.getKey();
            if (name.equals("indices")) {
                if (entry.getValue() instanceof String) {
                    indices(Strings.splitStringByCommaToArray((String) entry.getValue()));
                } else if (entry.getValue() instanceof ArrayList) {
                    indices((ArrayList<String>) entry.getValue());
                } else {
                    throw new IllegalArgumentException("malformed indices section, should be an array of strings");
                }
            } else if (name.equals("feature_states")) {
                if (entry.getValue() instanceof List) {
                    featureStates((List<String>) entry.getValue());
                } else {
                    throw new IllegalArgumentException("malformed feature_states section, should be an array of strings");
                }
            } else if (name.equals("partial")) {
                partial(nodeBooleanValue(entry.getValue(), "partial"));
            } else if (name.equals("include_global_state")) {
                includeGlobalState = nodeBooleanValue(entry.getValue(), "include_global_state");
            } else if (name.equals("include_aliases")) {
                includeAliases = nodeBooleanValue(entry.getValue(), "include_aliases");
            } else if (name.equals("rename_pattern")) {
                if (entry.getValue() instanceof String) {
                    renamePattern((String) entry.getValue());
                } else {
                    throw new IllegalArgumentException("malformed rename_pattern");
                }
            } else if (name.equals("rename_replacement")) {
                if (entry.getValue() instanceof String) {
                    renameReplacement((String) entry.getValue());
                } else {
                    throw new IllegalArgumentException("malformed rename_replacement");
                }
            } else if (name.equals("index_settings")) {
                if ((entry.getValue() instanceof Map) == false) {
                    throw new IllegalArgumentException("malformed index_settings section");
                }
                indexSettings((Map<String, Object>) entry.getValue());
            } else if (name.equals("ignore_index_settings")) {
                if (entry.getValue() instanceof String) {
                    ignoreIndexSettings(Strings.splitStringByCommaToArray((String) entry.getValue()));
                } else if (entry.getValue() instanceof List) {
                    ignoreIndexSettings((List<String>) entry.getValue());
                } else {
                    throw new IllegalArgumentException("malformed ignore_index_settings section, should be an array of strings");
                }
            } else {
                if (IndicesOptions.isIndicesOptions(name) == false) {
                    throw new IllegalArgumentException("Unknown parameter " + name);
                }
            }
        }
        indicesOptions(IndicesOptions.fromMap(source, indicesOptions));
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        toXContentFragment(builder, params);
        builder.endObject();
        return builder;
    }

    private void toXContentFragment(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("indices");
        for (String index : indices) {
            builder.value(index);
        }
        builder.endArray();
        if (indicesOptions != null) {
            indicesOptions.toXContent(builder, params);
        }
        if (renamePattern != null) {
            builder.field("rename_pattern", renamePattern);
        }
        if (renameReplacement != null) {
            builder.field("rename_replacement", renameReplacement);
        }
        if (featureStates != null && featureStates.length > 0) {
            builder.startArray("feature_states");
            for (String plugin : featureStates) {
                builder.value(plugin);
            }
            builder.endArray();
        }
        builder.field("include_global_state", includeGlobalState);
        builder.field("partial", partial);
        builder.field("include_aliases", includeAliases);
        if (indexSettings != null) {
            builder.startObject("index_settings");
            if (indexSettings.isEmpty() == false) {
                indexSettings.toXContent(builder, params);
            }
            builder.endObject();
        }
        builder.startArray("ignore_index_settings");
        for (String ignoreIndexSetting : ignoreIndexSettings) {
            builder.value(ignoreIndexSetting);
        }
        builder.endArray();
    }

    @Override
    public String getDescription() {
        return "snapshot [" + repository + ":" + snapshot + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RestoreSnapshotRequest that = (RestoreSnapshotRequest) o;
        return waitForCompletion == that.waitForCompletion
            && includeGlobalState == that.includeGlobalState
            && partial == that.partial
            && includeAliases == that.includeAliases
            && Objects.equals(snapshot, that.snapshot)
            && Objects.equals(repository, that.repository)
            && Arrays.equals(indices, that.indices)
            && Objects.equals(indicesOptions, that.indicesOptions)
            && Arrays.equals(featureStates, that.featureStates)
            && Objects.equals(renamePattern, that.renamePattern)
            && Objects.equals(renameReplacement, that.renameReplacement)
            && Objects.equals(indexSettings, that.indexSettings)
            && Arrays.equals(ignoreIndexSettings, that.ignoreIndexSettings)
            && Objects.equals(snapshotUuid, that.snapshotUuid)
            && skipOperatorOnlyState == that.skipOperatorOnlyState;
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(
            snapshot,
            repository,
            indicesOptions,
            renamePattern,
            renameReplacement,
            waitForCompletion,
            includeGlobalState,
            partial,
            includeAliases,
            indexSettings,
            snapshotUuid,
            skipOperatorOnlyState
        );
        result = 31 * result + Arrays.hashCode(indices);
        result = 31 * result + Arrays.hashCode(ignoreIndexSettings);
        result = 31 * result + Arrays.hashCode(featureStates);
        return result;
    }

    @Override
    public String toString() {
        return Strings.toString((ToXContentObject) (builder, params) -> {
            builder.startObject();
            toXContentFragment(builder, params);
            builder.field("skipOperatorOnlyState", skipOperatorOnlyState);
            builder.endObject();
            return builder;
        });
    }
}
