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

package org.elasticsearch.action.admin.cluster.snapshots.restore;

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeOperationRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.Strings.hasLength;
import static org.elasticsearch.common.settings.ImmutableSettings.Builder.EMPTY_SETTINGS;
import static org.elasticsearch.common.settings.ImmutableSettings.readSettingsFromStream;
import static org.elasticsearch.common.settings.ImmutableSettings.writeSettingsToStream;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;

/**
 * Restore snapshot request
 */
public class RestoreSnapshotRequest extends MasterNodeOperationRequest<RestoreSnapshotRequest> {

    private String snapshot;

    private String repository;

    private String[] indices = Strings.EMPTY_ARRAY;

    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpen();

    private String renamePattern;

    private String renameReplacement;

    private boolean waitForCompletion;

    private boolean includeGlobalState = true;

    private boolean partial = false;

    private boolean includeAliases = true;

    private Settings settings = EMPTY_SETTINGS;

    RestoreSnapshotRequest() {
    }

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
        if (settings == null) {
            validationException = addValidationError("settings are missing", validationException);
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
     * <p/>
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
     * <p/>
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
     *
     * @return
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
     * <p/>
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
     * <p/>
     * See {@link #renamePattern(String)} for more information.
     *
     * @param renameReplacement rename replacement
     * @return
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
     * Sets repository-specific restore settings.
     * <p/>
     * See repository documentation for more information.
     *
     * @param settings repository-specific snapshot settings
     * @return this request
     */
    public RestoreSnapshotRequest settings(Settings settings) {
        this.settings = settings;
        return this;
    }

    /**
     * Sets repository-specific restore settings.
     * <p/>
     * See repository documentation for more information.
     *
     * @param settings repository-specific snapshot settings
     * @return this request
     */
    public RestoreSnapshotRequest settings(Settings.Builder settings) {
        this.settings = settings.build();
        return this;
    }

    /**
     * Sets repository-specific restore settings in JSON, YAML or properties format
     * <p/>
     * See repository documentation for more information.
     *
     * @param source repository-specific snapshot settings
     * @return this request
     */
    public RestoreSnapshotRequest settings(String source) {
        this.settings = ImmutableSettings.settingsBuilder().loadFromSource(source).build();
        return this;
    }

    /**
     * Sets repository-specific restore settings
     * <p/>
     * See repository documentation for more information.
     *
     * @param source repository-specific snapshot settings
     * @return this request
     */
    public RestoreSnapshotRequest settings(Map<String, Object> source) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.map(source);
            settings(builder.string());
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
        return this;
    }

    /**
     * Returns repository-specific restore settings
     *
     * @return restore settings
     */
    public Settings settings() {
        return this.settings;
    }

    /**
     * If set to true the restore procedure will restore global cluster state.
     * <p/>
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
     * Parses restore definition
     *
     * @param source restore definition
     * @return this request
     */
    public RestoreSnapshotRequest source(XContentBuilder source) {
        try {
            return source(source.bytes());
        } catch (Exception e) {
            throw new ElasticsearchIllegalArgumentException("Failed to build json for repository request", e);
        }
    }

    /**
     * Parses restore definition
     *
     * @param source restore definition
     * @return this request
     */
    public RestoreSnapshotRequest source(Map source) {
        boolean ignoreUnavailable = IndicesOptions.lenientExpandOpen().ignoreUnavailable();
        boolean allowNoIndices = IndicesOptions.lenientExpandOpen().allowNoIndices();
        boolean expandWildcardsOpen = IndicesOptions.lenientExpandOpen().expandWildcardsOpen();
        boolean expandWildcardsClosed = IndicesOptions.lenientExpandOpen().expandWildcardsClosed();

        for (Map.Entry<String, Object> entry : ((Map<String, Object>) source).entrySet()) {
            String name = entry.getKey();
            if (name.equals("indices")) {
                if (entry.getValue() instanceof String) {
                    indices(Strings.splitStringByCommaToArray((String) entry.getValue()));
                } else if (entry.getValue() instanceof ArrayList) {
                    indices((ArrayList<String>) entry.getValue());
                } else {
                    throw new ElasticsearchIllegalArgumentException("malformed indices section, should be an array of strings");
                }
            } else if (name.equals("ignore_unavailable") || name.equals("ignoreUnavailable")) {
                ignoreUnavailable = nodeBooleanValue(entry.getValue());
            } else if (name.equals("allow_no_indices") || name.equals("allowNoIndices")) {
                allowNoIndices = nodeBooleanValue(entry.getValue());
            } else if (name.equals("expand_wildcards_open") || name.equals("expandWildcardsOpen")) {
                expandWildcardsOpen = nodeBooleanValue(entry.getValue());
            } else if (name.equals("expand_wildcards_closed") || name.equals("expandWildcardsClosed")) {
                expandWildcardsClosed = nodeBooleanValue(entry.getValue());
            } else if (name.equals("partial")) {
                partial(nodeBooleanValue(entry.getValue()));
            } else if (name.equals("settings")) {
                if (!(entry.getValue() instanceof Map)) {
                    throw new ElasticsearchIllegalArgumentException("malformed settings section, should indices an inner object");
                }
                settings((Map<String, Object>) entry.getValue());
            } else if (name.equals("include_global_state")) {
                includeGlobalState = nodeBooleanValue(entry.getValue());
            } else if (name.equals("include_aliases")) {
                includeAliases = nodeBooleanValue(entry.getValue());
            } else if (name.equals("rename_pattern")) {
                if (entry.getValue() instanceof String) {
                    renamePattern((String) entry.getValue());
                } else {
                    throw new ElasticsearchIllegalArgumentException("malformed rename_pattern");
                }
            } else if (name.equals("rename_replacement")) {
                if (entry.getValue() instanceof String) {
                    renameReplacement((String) entry.getValue());
                } else {
                    throw new ElasticsearchIllegalArgumentException("malformed rename_replacement");
                }
            } else {
                throw new ElasticsearchIllegalArgumentException("Unknown parameter " + name);
            }
        }
        indicesOptions(IndicesOptions.fromOptions(ignoreUnavailable, allowNoIndices, expandWildcardsOpen, expandWildcardsClosed));
        return this;
    }

    /**
     * Parses restore definition
     * <p/>
     * JSON, YAML and properties formats are supported
     *
     * @param source restore definition
     * @return this request
     */
    public RestoreSnapshotRequest source(String source) {
        if (hasLength(source)) {
            try {
                return source(XContentFactory.xContent(source).createParser(source).mapOrderedAndClose());
            } catch (Exception e) {
                throw new ElasticsearchIllegalArgumentException("failed to parse repository source [" + source + "]", e);
            }
        }
        return this;
    }

    /**
     * Parses restore definition
     * <p/>
     * JSON, YAML and properties formats are supported
     *
     * @param source restore definition
     * @return this request
     */
    public RestoreSnapshotRequest source(byte[] source) {
        return source(source, 0, source.length);
    }

    /**
     * Parses restore definition
     * <p/>
     * JSON, YAML and properties formats are supported
     *
     * @param source restore definition
     * @param offset offset
     * @param length length
     * @return this request
     */
    public RestoreSnapshotRequest source(byte[] source, int offset, int length) {
        if (length > 0) {
            try {
                return source(XContentFactory.xContent(source, offset, length).createParser(source, offset, length).mapOrderedAndClose());
            } catch (IOException e) {
                throw new ElasticsearchIllegalArgumentException("failed to parse repository source", e);
            }
        }
        return this;
    }

    /**
     * Parses restore definition
     * <p/>
     * JSON, YAML and properties formats are supported
     *
     * @param source restore definition
     * @return this request
     */
    public RestoreSnapshotRequest source(BytesReference source) {
        try {
            return source(XContentFactory.xContent(source).createParser(source).mapOrderedAndClose());
        } catch (IOException e) {
            throw new ElasticsearchIllegalArgumentException("failed to parse template source", e);
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        snapshot = in.readString();
        repository = in.readString();
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        renamePattern = in.readOptionalString();
        renameReplacement = in.readOptionalString();
        waitForCompletion = in.readBoolean();
        includeGlobalState = in.readBoolean();
        if (in.getVersion().onOrAfter(Version.V_1_3_0)) {
            partial = in.readBoolean();
            includeAliases = in.readBoolean();
        }
        settings = readSettingsFromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(snapshot);
        out.writeString(repository);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
        out.writeOptionalString(renamePattern);
        out.writeOptionalString(renameReplacement);
        out.writeBoolean(waitForCompletion);
        out.writeBoolean(includeGlobalState);
        if (out.getVersion().onOrAfter(Version.V_1_3_0)) {
            out.writeBoolean(partial);
            out.writeBoolean(includeAliases);
        }
        writeSettingsToStream(settings, out);
    }
}
