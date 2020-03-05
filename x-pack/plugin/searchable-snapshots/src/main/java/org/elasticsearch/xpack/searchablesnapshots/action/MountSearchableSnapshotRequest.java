/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.common.settings.Settings.readSettingsFromStream;
import static org.elasticsearch.common.settings.Settings.writeSettingsToStream;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class MountSearchableSnapshotRequest extends MasterNodeRequest<MountSearchableSnapshotRequest> implements ToXContentObject {

    public static final ConstructingObjectParser<MountSearchableSnapshotRequest, RequestParams> PARSER = new ConstructingObjectParser<>(
        "mount_searchable_snapshot", true,
        (a, p) -> new MountSearchableSnapshotRequest(p.mountedIndexName, (String) a[0], (String) a[1], (String) a[2],
            (Settings) a[3], (Settings) a[4], (String[]) a[5], p.masterTimeout, p.waitForCompletion));

    private static final ParseField REPOSITORY_FIELD = new ParseField("repository");
    private static final ParseField SNAPSHOT_FIELD = new ParseField("snapshot");
    private static final ParseField SNAPSHOT_INDEX_FIELD = new ParseField("index");
    private static final ParseField SETTINGS_FIELD = new ParseField("settings");
    private static final ParseField INDEX_SETTINGS_FIELD = new ParseField("index_settings");
    private static final ParseField IGNORE_INDEX_SETTINGS_FIELD = new ParseField("ignore_index_settings");

    static {
        PARSER.declareField(constructorArg(), XContentParser::text, REPOSITORY_FIELD, ObjectParser.ValueType.STRING);
        PARSER.declareField(constructorArg(), XContentParser::text, SNAPSHOT_FIELD, ObjectParser.ValueType.STRING);
        PARSER.declareField(constructorArg(), XContentParser::text, SNAPSHOT_INDEX_FIELD, ObjectParser.ValueType.STRING);
        PARSER.declareField(optionalConstructorArg(), Settings::fromXContent, SETTINGS_FIELD, ObjectParser.ValueType.OBJECT);
        PARSER.declareField(optionalConstructorArg(), Settings::fromXContent, INDEX_SETTINGS_FIELD, ObjectParser.ValueType.OBJECT);
        PARSER.declareField(optionalConstructorArg(),
            p -> p.list().stream().map(s -> (String) s).collect(Collectors.toList()).toArray(Strings.EMPTY_ARRAY),
            IGNORE_INDEX_SETTINGS_FIELD, ObjectParser.ValueType.STRING_ARRAY);
    }

    public static class RequestParams {
        final String mountedIndexName;
        final TimeValue masterTimeout;
        final boolean waitForCompletion;

        public RequestParams(String mountedIndexName, TimeValue masterTimeout, boolean waitForCompletion) {
            this.mountedIndexName = mountedIndexName;
            this.masterTimeout = masterTimeout;
            this.waitForCompletion = waitForCompletion;
        }
    }

    private final String repository;
    private final String snapshot;
    private final String snapshotIndex;
    private final String mountedIndex;
    private final boolean waitForCompletion;
    private final Settings settings;
    private final Settings indexSettings;
    private final String[] ignoreIndexSettings;

    /**
     * Constructs a new mount searchable snapshot request, restoring an index under the given mountedIndex name
     */
    public MountSearchableSnapshotRequest(String mountedIndexName,
                                          String repository, String snapshotName, String snapshotIndexName,
                                          Settings settings, Settings indexSettings,
                                          String[] ignoreIndexSettings, TimeValue masterNodeTimeout, boolean waitForCompletion) {
        this.repository = Objects.requireNonNull(repository);
        this.snapshot = Objects.requireNonNull(snapshotName);
        this.snapshotIndex = Objects.requireNonNull(snapshotIndexName);
        this.mountedIndex = Objects.requireNonNull(mountedIndexName);
        this.settings = Objects.requireNonNullElse(settings, Settings.EMPTY);
        this.indexSettings = Objects.requireNonNullElse(indexSettings, Settings.EMPTY);
        this.ignoreIndexSettings = Objects.requireNonNullElse(ignoreIndexSettings, Strings.EMPTY_ARRAY);
        this.waitForCompletion = waitForCompletion;
        masterNodeTimeout(Objects.requireNonNull(masterNodeTimeout));
    }

    public MountSearchableSnapshotRequest(StreamInput in) throws IOException {
        super(in);
        this.snapshot = in.readString();
        this.repository = in.readString();
        this.snapshotIndex = in.readString();
        this.mountedIndex = in.readString();
        this.waitForCompletion = in.readBoolean();
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
        out.writeBoolean(waitForCompletion);
        writeSettingsToStream(settings, out);
        writeSettingsToStream(indexSettings, out);
        out.writeStringArray(ignoreIndexSettings);
    }

    @Override
    public ActionRequestValidationException validate() {
       return null;
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
     * Returns the name of the snapshot.
     *
     * @return snapshot name
     */
    public String snapshot() {
        return this.snapshot;
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
     * Returns wait for completion setting
     *
     * @return true if the operation will wait for completion
     */
    public boolean waitForCompletion() {
        return waitForCompletion;
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
     * Returns the list of index settings and index settings groups that shouldn't be restored from snapshot
     */
    public String[] ignoreIndexSettings() {
        return ignoreIndexSettings;
    }

    /**
     * Returns settings that should be added/changed for restored index
     */
    public Settings indexSettings() {
        return this.indexSettings;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(REPOSITORY_FIELD.getPreferredName(), repository);
        builder.field(SNAPSHOT_FIELD.getPreferredName(), snapshot);
        builder.field(SNAPSHOT_INDEX_FIELD.getPreferredName(), snapshotIndex);
        if (settings.isEmpty() == false) {
            builder.startObject(SETTINGS_FIELD.getPreferredName());
            settings.toXContent(builder, params);
            builder.endObject();
        }
        if (indexSettings.isEmpty() == false) {
            builder.startObject(INDEX_SETTINGS_FIELD.getPreferredName());
            indexSettings.toXContent(builder, params);
            builder.endObject();
        }
        if (ignoreIndexSettings.length > 0) {
            builder.startArray(IGNORE_INDEX_SETTINGS_FIELD.getPreferredName());
            for (String ignoreIndexSetting : ignoreIndexSettings) {
                builder.value(ignoreIndexSetting);
            }
            builder.endArray();
        }
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
            Arrays.equals(ignoreIndexSettings, that.ignoreIndexSettings) &&
            Objects.equals(masterNodeTimeout, that.masterNodeTimeout);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(snapshot, repository, snapshotIndex, mountedIndex, waitForCompletion, settings, indexSettings,
            masterNodeTimeout);
        result = 31 * result + Arrays.hashCode(ignoreIndexSettings);
        return result;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
