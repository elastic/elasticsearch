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
            (Settings) a[3], (String[]) a[4], p.waitForCompletion));

    private static final ParseField REPOSITORY_FIELD = new ParseField("repository");
    private static final ParseField SNAPSHOT_FIELD = new ParseField("snapshot");
    private static final ParseField SNAPSHOT_INDEX_FIELD = new ParseField("index");
    private static final ParseField INDEX_SETTINGS_FIELD = new ParseField("index_settings");
    private static final ParseField IGNORE_INDEX_SETTINGS_FIELD = new ParseField("ignore_index_settings");

    static {
        PARSER.declareField(constructorArg(), XContentParser::text, REPOSITORY_FIELD, ObjectParser.ValueType.STRING);
        PARSER.declareField(constructorArg(), XContentParser::text, SNAPSHOT_FIELD, ObjectParser.ValueType.STRING);
        PARSER.declareField(constructorArg(), XContentParser::text, SNAPSHOT_INDEX_FIELD, ObjectParser.ValueType.STRING);
        PARSER.declareField(optionalConstructorArg(), Settings::fromXContent, INDEX_SETTINGS_FIELD, ObjectParser.ValueType.OBJECT);
        PARSER.declareField(optionalConstructorArg(),
            p -> p.list().stream().map(s -> (String) s).collect(Collectors.toList()).toArray(Strings.EMPTY_ARRAY),
            IGNORE_INDEX_SETTINGS_FIELD, ObjectParser.ValueType.STRING_ARRAY);
    }

    public static class RequestParams {
        final String mountedIndexName;
        final boolean waitForCompletion;

        public RequestParams(String mountedIndexName, boolean waitForCompletion) {
            this.mountedIndexName = mountedIndexName;
            this.waitForCompletion = waitForCompletion;
        }
    }

    private final String mountedIndexName;
    private final String repositoryName;
    private final String snapshotName;
    private final String snapshotIndexName;
    private final Settings indexSettings;
    private final String[] ignoredIndexSettings;
    private final boolean waitForCompletion;

    /**
     * Constructs a new mount searchable snapshot request, restoring an index with the settings needed to make it a searchable snapshot.
     */
    public MountSearchableSnapshotRequest(String mountedIndexName,
                                          String repositoryName, String snapshotName, String snapshotIndexName, Settings indexSettings,
                                          String[] ignoredIndexSettings, boolean waitForCompletion) {
        this.mountedIndexName = Objects.requireNonNull(mountedIndexName);
        this.repositoryName = Objects.requireNonNull(repositoryName);
        this.snapshotName = Objects.requireNonNull(snapshotName);
        this.snapshotIndexName = Objects.requireNonNull(snapshotIndexName);
        this.indexSettings = Objects.requireNonNullElse(indexSettings, Settings.EMPTY);
        this.ignoredIndexSettings = Objects.requireNonNullElse(ignoredIndexSettings, Strings.EMPTY_ARRAY);
        this.waitForCompletion = waitForCompletion;
    }

    MountSearchableSnapshotRequest(StreamInput in) throws IOException {
        super(in);
        this.mountedIndexName = in.readString();
        this.repositoryName = in.readString();
        this.snapshotName = in.readString();
        this.snapshotIndexName = in.readString();
        this.indexSettings = readSettingsFromStream(in);
        this.ignoredIndexSettings = in.readStringArray();
        this.waitForCompletion = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(mountedIndexName);
        out.writeString(repositoryName);
        out.writeString(snapshotName);
        out.writeString(snapshotIndexName);
        writeSettingsToStream(indexSettings, out);
        out.writeStringArray(ignoredIndexSettings);
        out.writeBoolean(waitForCompletion);
    }

    @Override
    public ActionRequestValidationException validate() {
       return null;
    }

    /**
     * @return the name of the index that will be created
     * @noinspection WeakerAccess
     */
    public String mountedIndexName() {
        return mountedIndexName;
    }

    /**
     * @return the name of the repository
     * @noinspection WeakerAccess
     */
    public String repositoryName() {
        return this.repositoryName;
    }

    /**
     * @return the name of the snapshot.
     * @noinspection WeakerAccess
     */
    public String snapshotName() {
        return this.snapshotName;
    }

    /**
     * @return the name of the index contained in the snapshot
     * @noinspection WeakerAccess
     */
    public String snapshotIndexName() {
        return snapshotIndexName;
    }

    /**
     * @return true if the operation will wait for completion
     * @noinspection WeakerAccess
     */
    public boolean waitForCompletion() {
        return waitForCompletion;
    }

    /**
     * @return settings that should be added to the index when it is mounted
     * @noinspection WeakerAccess
     */
    public Settings indexSettings() {
        return this.indexSettings;
    }

    /**
     * @return the names of settings that should be removed from the index when it is mounted
     * @noinspection WeakerAccess
     */
    public String[] ignoreIndexSettings() {
        return ignoredIndexSettings;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(REPOSITORY_FIELD.getPreferredName(), repositoryName);
        builder.field(SNAPSHOT_FIELD.getPreferredName(), snapshotName);
        builder.field(SNAPSHOT_INDEX_FIELD.getPreferredName(), snapshotIndexName);
        if (indexSettings.isEmpty() == false) {
            builder.startObject(INDEX_SETTINGS_FIELD.getPreferredName());
            indexSettings.toXContent(builder, params);
            builder.endObject();
        }
        if (ignoredIndexSettings.length > 0) {
            builder.startArray(IGNORE_INDEX_SETTINGS_FIELD.getPreferredName());
            for (String ignoreIndexSetting : ignoredIndexSettings) {
                builder.value(ignoreIndexSetting);
            }
            builder.endArray();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String getDescription() {
        return "mount snapshot [" + repositoryName + ":" + snapshotName + ":" + snapshotIndexName + "->" + mountedIndexName + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MountSearchableSnapshotRequest that = (MountSearchableSnapshotRequest) o;
        return waitForCompletion == that.waitForCompletion &&
            Objects.equals(mountedIndexName, that.mountedIndexName) &&
            Objects.equals(repositoryName, that.repositoryName) &&
            Objects.equals(snapshotName, that.snapshotName) &&
            Objects.equals(snapshotIndexName, that.snapshotIndexName) &&
            Objects.equals(indexSettings, that.indexSettings) &&
            Arrays.equals(ignoredIndexSettings, that.ignoredIndexSettings) &&
            Objects.equals(masterNodeTimeout, that.masterNodeTimeout);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(mountedIndexName, repositoryName, snapshotName, snapshotIndexName, indexSettings, waitForCompletion,
            masterNodeTimeout);
        result = 31 * result + Arrays.hashCode(ignoredIndexSettings);
        return result;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
