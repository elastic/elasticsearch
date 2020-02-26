/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots.action;

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.settings.Settings.Builder.EMPTY_SETTINGS;
import static org.elasticsearch.common.settings.Settings.readSettingsFromStream;
import static org.elasticsearch.common.settings.Settings.writeSettingsToStream;

public class MountSearchableSnapshotRequest extends MasterNodeRequest<MountSearchableSnapshotRequest> implements ToXContentObject {

    private final String mountedIndex;
    private String snapshot;
    private String repository;
    private String snapshotIndex;
    private Boolean waitForCompletion;
    private Settings settings = EMPTY_SETTINGS;
    private Settings indexSettings = EMPTY_SETTINGS;
    private String[] ignoreIndexSettings = Strings.EMPTY_ARRAY;

    /**
     * Constructs a new put repository request with the provided repository and snapshot names.
     */
    public MountSearchableSnapshotRequest(String mountedIndex) {
        this.mountedIndex = mountedIndex;
    }

    public MountSearchableSnapshotRequest(StreamInput in) throws IOException {
        super(in);
        this.snapshot = in.readString();
        this.repository = in.readString();
        this.snapshotIndex = in.readString();
        this.mountedIndex = in.readString();
        this.waitForCompletion = in.readOptionalBoolean();
        this.settings = readSettingsFromStream(in);
        this.indexSettings = readSettingsFromStream(in);
        this.ignoreIndexSettings = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(snapshot);
        out.writeString(repository);
        out.writeString(snapshotIndex);
        out.writeString(mountedIndex);
        out.writeOptionalBoolean(waitForCompletion);
        writeSettingsToStream(settings, out);
        writeSettingsToStream(indexSettings, out);
        out.writeStringArray(ignoreIndexSettings);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (snapshot == null) {
            validationException = addValidationError("snapshot name is missing", validationException);
        }
        if (repository == null) {
            validationException = addValidationError("repository is missing", validationException);
        }
        if (snapshotIndex == null) {
            validationException = addValidationError("index name from snapshot is missing", validationException);
        }
        if (mountedIndex == null) {
            validationException = addValidationError("index name to mount is missing", validationException);
        }
        if (settings == null) {
            validationException = addValidationError("settings are missing", validationException);
        }
        if (indexSettings == null) {
            validationException = addValidationError("indexSettings are missing", validationException);
        }
        return validationException;
    }

    /**
     * Sets the name of the snapshot.
     *
     * @param snapshot snapshot name
     * @return this request
     */
    public MountSearchableSnapshotRequest snapshot(String snapshot) {
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
    public MountSearchableSnapshotRequest repository(String repository) {
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
     * Sets the name of the index that should be restored from snapshot
     */
    public MountSearchableSnapshotRequest snapshotIndex(String indexName) {
        this.snapshotIndex = indexName;
        return this;
    }

    /**
     * Returns index that should be restored from snapshot
     */
    public String snapshotIndex() {
        return snapshotIndex;
    }

    /**
     * Returns new index name after the snapshot has been mounted
     */
    public String mountedIndex() {
        return mountedIndex;
    }

    /**
     * If this parameter is set to true the operation will wait for completion of restore process before returning.
     *
     * @param waitForCompletion if true the operation will wait for completion
     * @return this request
     */
    public MountSearchableSnapshotRequest waitForCompletion(boolean waitForCompletion) {
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
     * Sets repository-specific restore settings.
     * <p>
     * See repository documentation for more information.
     *
     * @param settings repository-specific snapshot settings
     * @return this request
     */
    public MountSearchableSnapshotRequest settings(Settings settings) {
        this.settings = settings;
        return this;
    }

    /**
     * Sets repository-specific restore settings.
     * <p>
     * See repository documentation for more information.
     *
     * @param settings repository-specific snapshot settings
     * @return this request
     */
    public MountSearchableSnapshotRequest settings(Settings.Builder settings) {
        this.settings = settings.build();
        return this;
    }

    /**
     * Sets repository-specific restore settings in JSON or YAML format
     * <p>
     * See repository documentation for more information.
     *
     * @param source repository-specific snapshot settings
     * @param xContentType the content type of the source
     * @return this request
     */
    public MountSearchableSnapshotRequest settings(String source, XContentType xContentType) {
        this.settings = Settings.builder().loadFromSource(source, xContentType).build();
        return this;
    }

    /**
     * Sets repository-specific restore settings
     * <p>
     * See repository documentation for more information.
     *
     * @param source repository-specific snapshot settings
     * @return this request
     */
    public MountSearchableSnapshotRequest settings(Map<String, Object> source) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.map(source);
            settings(Strings.toString(builder), builder.contentType());
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
     * Sets the list of index settings and index settings groups that shouldn't be restored from snapshot
     */
    public MountSearchableSnapshotRequest ignoreIndexSettings(String... ignoreIndexSettings) {
        this.ignoreIndexSettings = ignoreIndexSettings;
        return this;
    }

    /**
     * Sets the list of index settings and index settings groups that shouldn't be restored from snapshot
     */
    public MountSearchableSnapshotRequest ignoreIndexSettings(List<String> ignoreIndexSettings) {
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
     * Sets settings that should be added/changed for restored index
     */
    public MountSearchableSnapshotRequest indexSettings(Settings settings) {
        this.indexSettings = settings;
        return this;
    }

    /**
     * Sets settings that should be added/changed for restored index
     */
    public MountSearchableSnapshotRequest indexSettings(Settings.Builder settings) {
        this.indexSettings = settings.build();
        return this;
    }

    /**
     * Sets settings that should be added/changed for restored index
     */
    public MountSearchableSnapshotRequest indexSettings(String source, XContentType xContentType) {
        this.indexSettings = Settings.builder().loadFromSource(source, xContentType).build();
        return this;
    }

    /**
     * Sets settings that should be added/changed for restored index
     */
    public MountSearchableSnapshotRequest indexSettings(Map<String, Object> source) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.map(source);
            indexSettings(Strings.toString(builder), builder.contentType());
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
        return this;
    }

    /**
     * Returns settings that should be added/changed for restored index
     */
    public Settings indexSettings() {
        return this.indexSettings;
    }

    /**
     * Parses mount definition
     *
     * @param source mount definition
     * @return this request
     */
    @SuppressWarnings("unchecked")
    public MountSearchableSnapshotRequest source(Map<String, Object> source) {
        // TODO: this could be rewritten with a Constructing Object Parser rather than manually
        //  parsing the XContent, this was copied/modified from RestoreSnapshotRequest
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            String name = entry.getKey();
            if (name.equals("snapshot_index")) {
                if (entry.getValue() instanceof String) {
                    snapshotIndex((String) entry.getValue());
                } else {
                    throw new IllegalArgumentException("malformed snapshot_index section, should be a single string but was [" +
                        entry.getValue() + "]");
                }
            } else if (name.equals("repository")) {
                if (!(entry.getValue() instanceof String)) {
                    throw new IllegalArgumentException("malformed repository");
                }
                repository((String) entry.getValue());
            } else if (name.equals("snapshot")) {
                if (!(entry.getValue() instanceof String)) {
                    throw new IllegalArgumentException("malformed snapshot");
                }
                snapshot((String) entry.getValue());
            } else if (name.equals("settings")) {
                if (!(entry.getValue() instanceof Map)) {
                    throw new IllegalArgumentException("malformed settings section");
                }
                settings((Map<String, Object>) entry.getValue());
            } else if (name.equals("index_settings")) {
                if (!(entry.getValue() instanceof Map)) {
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
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("snapshot_index", snapshotIndex);
        builder.field("repository", repository);
        builder.field("snapshot", snapshot);
        if (settings != null) {
            builder.startObject("settings");
            if (settings.isEmpty() == false) {
                settings.toXContent(builder, params);
            }
            builder.endObject();
        }
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
        builder.endObject();
        return builder;
    }

    @Override
    public String getDescription() {
        return "mount snapshot [" + repository + ":" + snapshot + ":" + snapshotIndex + "->" + mountedIndex + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MountSearchableSnapshotRequest that = (MountSearchableSnapshotRequest) o;
        return waitForCompletion == that.waitForCompletion &&
            Objects.equals(snapshot, that.snapshot) &&
            Objects.equals(repository, that.repository) &&
            Objects.equals(snapshotIndex, that.snapshotIndex) &&
            Objects.equals(mountedIndex, that.mountedIndex) &&
            Objects.equals(settings, that.settings) &&
            Objects.equals(indexSettings, that.indexSettings) &&
            Arrays.equals(ignoreIndexSettings, that.ignoreIndexSettings);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(snapshot, repository, snapshotIndex, mountedIndex, waitForCompletion,
            settings, indexSettings);
        result = 31 * result + Arrays.hashCode(ignoreIndexSettings);
        return result;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
