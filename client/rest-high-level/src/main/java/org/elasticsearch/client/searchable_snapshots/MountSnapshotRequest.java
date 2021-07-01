/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.searchable_snapshots;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class MountSnapshotRequest implements ToXContentObject, Validatable {

    private final String repository;

    public String getRepository() {
        return repository;
    }

    private final String snapshot;

    public String getSnapshot() {
        return snapshot;
    }

    private final String index;

    public String getIndex() {
        return index;
    }

    public MountSnapshotRequest(final String repository, final String snapshot, final String index) {
        this.repository = Objects.requireNonNull(repository);
        this.snapshot = Objects.requireNonNull(snapshot);
        this.index = Objects.requireNonNull(index);
    }

    private TimeValue masterTimeout;

    public TimeValue getMasterTimeout() {
        return masterTimeout;
    }

    public MountSnapshotRequest masterTimeout(final TimeValue masterTimeout) {
        this.masterTimeout = masterTimeout;
        return this;
    }

    private Boolean waitForCompletion;

    public Boolean getWaitForCompletion() {
        return waitForCompletion;
    }

    public MountSnapshotRequest waitForCompletion(final boolean waitForCompletion) {
        this.waitForCompletion = waitForCompletion;
        return this;
    }

    public enum Storage {

        FULL_COPY("full_copy"),
        SHARED_CACHE("shared_cache");

        private final String storageName;

        public String storageName() {
            return storageName;
        }

        Storage(final String storageName) {
            this.storageName = storageName;
        }

    }

    private Storage storage;

    public Storage getStorage() {
        return storage;
    }

    public MountSnapshotRequest storage(final Storage storage) {
        this.storage = storage;
        return this;
    }

    private String renamedIndex;

    public String getRenamedIndex() {
        return renamedIndex;
    }

    public MountSnapshotRequest renamedIndex(final String renamedIndex) {
        this.renamedIndex = renamedIndex;
        return this;
    }

    private Settings indexSettings;

    public Settings getIndexSettings() {
        return indexSettings;
    }

    public MountSnapshotRequest indexSettings(final Settings indexSettings) {
        this.indexSettings = indexSettings;
        return this;
    }

    private String[] ignoredIndexSettings;

    public String[] getIgnoredIndexSettings() {
        return ignoredIndexSettings;
    }

    public MountSnapshotRequest ignoredIndexSettings(final String[] ignoredIndexSettings) {
        if (ignoredIndexSettings != null) {
            for (final String ignoredIndexSetting : ignoredIndexSettings) {
                Objects.requireNonNull(ignoredIndexSetting);
            }
        }
        this.ignoredIndexSettings = ignoredIndexSettings;
        return this;
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        {
            builder.field("index", index);
            if (renamedIndex != null) {
                builder.field("renamed_index", renamedIndex);
            }
            if (indexSettings != null) {
                builder.startObject("index_settings"); {
                    indexSettings.toXContent(builder, params);
                }
                builder.endObject();
            }
            if (ignoredIndexSettings != null) {
                builder.array("ignored_index_settings", ignoredIndexSettings);
            }
        }
        builder.endObject();
        return builder;
    }

}
