/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.searchablesnapshots;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.settings.Settings.readSettingsFromStream;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class MountSearchableSnapshotRequest extends MasterNodeRequest<MountSearchableSnapshotRequest> {

    public static final ConstructingObjectParser<MountSearchableSnapshotRequest, RestRequest> PARSER = new ConstructingObjectParser<>(
        "mount_searchable_snapshot",
        false,
        (a, request) -> new MountSearchableSnapshotRequest(
            Objects.requireNonNullElse((String) a[1], (String) a[0]),
            Objects.requireNonNull(request.param("repository")),
            Objects.requireNonNull(request.param("snapshot")),
            (String) a[0],
            Objects.requireNonNullElse((Settings) a[2], Settings.EMPTY),
            Objects.requireNonNullElse((String[]) a[3], Strings.EMPTY_ARRAY),
            request.paramAsBoolean("wait_for_completion", false),
            Storage.valueOf(request.param("storage", Storage.FULL_COPY.toString()).toUpperCase(Locale.ROOT))
        )
    );

    private static final ParseField INDEX_FIELD = new ParseField("index");
    private static final ParseField RENAMED_INDEX_FIELD = new ParseField("renamed_index");
    private static final ParseField INDEX_SETTINGS_FIELD = new ParseField("index_settings");
    private static final ParseField IGNORE_INDEX_SETTINGS_FIELD = new ParseField("ignore_index_settings");

    /**
     * This field only exists to be silently ignored when the body of a Mount API request contains a "ignored_index_settings" instead of
     * "ignore_index_settings" (note the missing 'd'). We need to silently ignores this field instead of rejecting the request because the
     * High Level REST Client uses the wrong field name. See https://github.com/elastic/elasticsearch/issues/75982.
     * TODO: remove in 9.0.
     */
    @Deprecated
    private static final ParseField IGNORED_INDEX_SETTINGS_FIELD = new ParseField("ignored_index_settings");

    static {
        PARSER.declareField(constructorArg(), XContentParser::text, INDEX_FIELD, ObjectParser.ValueType.STRING);
        PARSER.declareField(optionalConstructorArg(), XContentParser::text, RENAMED_INDEX_FIELD, ObjectParser.ValueType.STRING);
        PARSER.declareField(optionalConstructorArg(), Settings::fromXContent, INDEX_SETTINGS_FIELD, ObjectParser.ValueType.OBJECT);
        PARSER.declareField(
            optionalConstructorArg(),
            p -> p.list().stream().map(s -> (String) s).collect(Collectors.toList()).toArray(Strings.EMPTY_ARRAY),
            IGNORE_INDEX_SETTINGS_FIELD,
            ObjectParser.ValueType.STRING_ARRAY
        );
        PARSER.declareField(optionalConstructorArg(), (p, c) -> {
            p.skipChildren();
            return Strings.EMPTY_ARRAY;
        }, IGNORED_INDEX_SETTINGS_FIELD, ObjectParser.ValueType.STRING_ARRAY);
    }

    /**
     * Searchable snapshots partial storage was introduced in 7.12.0
     */
    private static final TransportVersion SHARED_CACHE_VERSION = TransportVersion.V_7_12_0;

    private final String mountedIndexName;
    private final String repositoryName;
    private final String snapshotName;
    private final String snapshotIndexName;
    private final Settings indexSettings;
    private final String[] ignoreIndexSettings;
    private final boolean waitForCompletion;
    private final Storage storage;

    /**
     * Constructs a new mount searchable snapshot request, restoring an index with the settings needed to make it a searchable snapshot.
     */
    public MountSearchableSnapshotRequest(
        String mountedIndexName,
        String repositoryName,
        String snapshotName,
        String snapshotIndexName,
        Settings indexSettings,
        String[] ignoreIndexSettings,
        boolean waitForCompletion,
        Storage storage
    ) {
        this.mountedIndexName = Objects.requireNonNull(mountedIndexName);
        this.repositoryName = Objects.requireNonNull(repositoryName);
        this.snapshotName = Objects.requireNonNull(snapshotName);
        this.snapshotIndexName = Objects.requireNonNull(snapshotIndexName);
        this.indexSettings = Objects.requireNonNull(indexSettings);
        this.ignoreIndexSettings = Objects.requireNonNull(ignoreIndexSettings);
        this.waitForCompletion = waitForCompletion;
        this.storage = storage;
    }

    public MountSearchableSnapshotRequest(StreamInput in) throws IOException {
        super(in);
        this.mountedIndexName = in.readString();
        this.repositoryName = in.readString();
        this.snapshotName = in.readString();
        this.snapshotIndexName = in.readString();
        this.indexSettings = readSettingsFromStream(in);
        this.ignoreIndexSettings = in.readStringArray();
        this.waitForCompletion = in.readBoolean();
        if (in.getTransportVersion().onOrAfter(SHARED_CACHE_VERSION)) {
            this.storage = Storage.readFromStream(in);
        } else {
            this.storage = Storage.FULL_COPY;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(mountedIndexName);
        out.writeString(repositoryName);
        out.writeString(snapshotName);
        out.writeString(snapshotIndexName);
        indexSettings.writeTo(out);
        out.writeStringArray(ignoreIndexSettings);
        out.writeBoolean(waitForCompletion);
        if (out.getTransportVersion().onOrAfter(SHARED_CACHE_VERSION)) {
            storage.writeTo(out);
        } else if (storage != Storage.FULL_COPY) {
            throw new UnsupportedOperationException(
                "storage type [" + storage + "] is not supported on version [" + out.getTransportVersion() + "]"
            );
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (IndexMetadata.INDEX_DATA_PATH_SETTING.exists(indexSettings)) {
            validationException = addValidationError(
                "setting [" + IndexMetadata.SETTING_DATA_PATH + "] is not permitted on searchable snapshots",
                validationException
            );
        }
        return validationException;
    }

    /**
     * @return the name of the index that will be created
     */
    public String mountedIndexName() {
        return mountedIndexName;
    }

    /**
     * @return the name of the repository
     */
    public String repositoryName() {
        return this.repositoryName;
    }

    /**
     * @return the name of the snapshot.
     */
    public String snapshotName() {
        return this.snapshotName;
    }

    /**
     * @return the name of the index contained in the snapshot
     */
    public String snapshotIndexName() {
        return snapshotIndexName;
    }

    /**
     * @return true if the operation will wait for completion
     */
    public boolean waitForCompletion() {
        return waitForCompletion;
    }

    /**
     * @return settings that should be added to the index when it is mounted
     */
    public Settings indexSettings() {
        return this.indexSettings;
    }

    /**
     * @return the names of settings that should be removed from the index when it is mounted
     */
    public String[] ignoreIndexSettings() {
        return ignoreIndexSettings;
    }

    /**
     * @return how nodes will use their local storage to accelerate searches of this snapshot
     */
    public Storage storage() {
        return storage;
    }

    @Override
    public String getDescription() {
        return "mount snapshot [" + repositoryName + ":" + snapshotName + ":" + snapshotIndexName + "] as [" + mountedIndexName + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MountSearchableSnapshotRequest that = (MountSearchableSnapshotRequest) o;
        return waitForCompletion == that.waitForCompletion
            && storage == that.storage
            && Objects.equals(mountedIndexName, that.mountedIndexName)
            && Objects.equals(repositoryName, that.repositoryName)
            && Objects.equals(snapshotName, that.snapshotName)
            && Objects.equals(snapshotIndexName, that.snapshotIndexName)
            && Objects.equals(indexSettings, that.indexSettings)
            && Arrays.equals(ignoreIndexSettings, that.ignoreIndexSettings)
            && Objects.equals(masterNodeTimeout, that.masterNodeTimeout);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(
            mountedIndexName,
            repositoryName,
            snapshotName,
            snapshotIndexName,
            indexSettings,
            waitForCompletion,
            masterNodeTimeout,
            storage
        );
        result = 31 * result + Arrays.hashCode(ignoreIndexSettings);
        return result;
    }

    @Override
    public String toString() {
        return getDescription();
    }

    /**
     * Enumerates the different ways that nodes can use their local storage to accelerate searches of a snapshot.
     */
    public enum Storage implements Writeable {
        FULL_COPY(String.join(",", DataTier.DATA_COLD, DataTier.DATA_WARM, DataTier.DATA_HOT)),
        SHARED_CACHE(DataTier.DATA_FROZEN);

        private final String defaultDataTiersPreference;

        Storage(String defaultDataTiersPreference) {
            this.defaultDataTiersPreference = defaultDataTiersPreference;
        }

        /**
         * Returns the default preference for new searchable snapshot indices. When
         * performing a full mount the preference is cold - warm - hot. When
         * performing a partial mount the preference is only frozen
         */
        public String defaultDataTiersPreference() {
            return defaultDataTiersPreference;
        }

        public static Storage fromString(String type) {
            if ("full_copy".equals(type)) {
                return FULL_COPY;
            } else if ("shared_cache".equals(type)) {
                return SHARED_CACHE;
            } else {
                throw new IllegalArgumentException(
                    "unknown searchable snapshot storage type ["
                        + type
                        + "], valid types are: "
                        + Strings.arrayToCommaDelimitedString(Storage.values())
                );
            }
        }

        @Override
        public String toString() {
            return super.toString().toLowerCase(Locale.ROOT);
        }

        public static Storage readFromStream(StreamInput in) throws IOException {
            return in.readEnum(Storage.class);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(this);
        }
    }
}
