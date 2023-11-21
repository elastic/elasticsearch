/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class ConnectorSyncInfo implements Writeable, ToXContentFragment {
    @Nullable
    private final String lastAccessControlSyncError;
    @Nullable
    private final String lastAccessControlSyncScheduledAt;
    @Nullable
    private final ConnectorSyncStatus lastAccessControlSyncStatus;
    @Nullable
    private final Long lastDeletedDocumentCount;
    @Nullable
    private final String lastIncrementalSyncScheduledAt;
    @Nullable
    private final Long lastIndexedDocumentCount;
    @Nullable
    private final String lastSeen;
    @Nullable
    private final String lastSyncError;
    @Nullable
    private final String lastSyncScheduledAt;
    @Nullable
    private final ConnectorSyncStatus lastSyncStatus;
    @Nullable
    private final String lastSynced;

    private ConnectorSyncInfo(
        String lastAccessControlSyncError,
        String lastAccessControlSyncScheduledAt,
        ConnectorSyncStatus lastAccessControlSyncStatus,
        Long lastDeletedDocumentCount,
        String lastIncrementalSyncScheduledAt,
        Long lastIndexedDocumentCount,
        String lastSeen,
        String lastSyncError,
        String lastSyncScheduledAt,
        ConnectorSyncStatus lastSyncStatus,
        String lastSynced
    ) {
        this.lastAccessControlSyncError = lastAccessControlSyncError;
        this.lastAccessControlSyncScheduledAt = lastAccessControlSyncScheduledAt;
        this.lastAccessControlSyncStatus = lastAccessControlSyncStatus;
        this.lastDeletedDocumentCount = lastDeletedDocumentCount;
        this.lastIncrementalSyncScheduledAt = lastIncrementalSyncScheduledAt;
        this.lastIndexedDocumentCount = lastIndexedDocumentCount;
        this.lastSeen = lastSeen;
        this.lastSyncError = lastSyncError;
        this.lastSyncScheduledAt = lastSyncScheduledAt;
        this.lastSyncStatus = lastSyncStatus;
        this.lastSynced = lastSynced;
    }

    public ConnectorSyncInfo(StreamInput in) throws IOException {
        this.lastAccessControlSyncError = in.readOptionalString();
        this.lastAccessControlSyncScheduledAt = in.readOptionalString();
        this.lastAccessControlSyncStatus = in.readOptionalEnum(ConnectorSyncStatus.class);
        this.lastDeletedDocumentCount = in.readOptionalLong();
        this.lastIncrementalSyncScheduledAt = in.readOptionalString();
        this.lastIndexedDocumentCount = in.readOptionalLong();
        this.lastSeen = in.readOptionalString();
        this.lastSyncError = in.readOptionalString();
        this.lastSyncScheduledAt = in.readOptionalString();
        this.lastSyncStatus = in.readOptionalEnum(ConnectorSyncStatus.class);
        this.lastSynced = in.readOptionalString();
    }

    public static final ParseField LAST_ACCESS_CONTROL_SYNC_ERROR = new ParseField("last_access_control_sync_error");
    public static final ParseField LAST_ACCESS_CONTROL_SYNC_STATUS_FIELD = new ParseField("last_access_control_sync_status");
    public static final ParseField LAST_ACCESS_CONTROL_SYNC_SCHEDULED_AT_FIELD = new ParseField("last_access_control_sync_scheduled_at");
    public static final ParseField LAST_DELETED_DOCUMENT_COUNT_FIELD = new ParseField("last_deleted_document_count");
    public static final ParseField LAST_INCREMENTAL_SYNC_SCHEDULED_AT_FIELD = new ParseField("last_incremental_sync_scheduled_at");
    public static final ParseField LAST_INDEXED_DOCUMENT_COUNT_FIELD = new ParseField("last_indexed_document_count");
    public static final ParseField LAST_SEEN_FIELD = new ParseField("last_seen");
    public static final ParseField LAST_SYNC_ERROR_FIELD = new ParseField("last_sync_error");
    public static final ParseField LAST_SYNC_SCHEDULED_AT_FIELD = new ParseField("last_sync_scheduled_at");
    public static final ParseField LAST_SYNC_STATUS_FIELD = new ParseField("last_sync_status");
    public static final ParseField LAST_SYNCED_FIELD = new ParseField("last_synced");

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(LAST_ACCESS_CONTROL_SYNC_ERROR.getPreferredName(), lastAccessControlSyncError);
        builder.field(LAST_ACCESS_CONTROL_SYNC_STATUS_FIELD.getPreferredName(), lastAccessControlSyncStatus);
        builder.field(LAST_ACCESS_CONTROL_SYNC_SCHEDULED_AT_FIELD.getPreferredName(), lastAccessControlSyncScheduledAt);
        builder.field(LAST_DELETED_DOCUMENT_COUNT_FIELD.getPreferredName(), lastDeletedDocumentCount);
        builder.field(LAST_INCREMENTAL_SYNC_SCHEDULED_AT_FIELD.getPreferredName(), lastIncrementalSyncScheduledAt);
        builder.field(LAST_INDEXED_DOCUMENT_COUNT_FIELD.getPreferredName(), lastIndexedDocumentCount);
        builder.field(LAST_SEEN_FIELD.getPreferredName(), lastSeen);
        builder.field(LAST_SYNC_ERROR_FIELD.getPreferredName(), lastSyncError);
        builder.field(LAST_SYNC_SCHEDULED_AT_FIELD.getPreferredName(), lastSyncScheduledAt);
        builder.field(LAST_SYNC_STATUS_FIELD.getPreferredName(), lastSyncStatus);
        builder.field(LAST_SYNCED_FIELD.getPreferredName(), lastSynced);
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(lastAccessControlSyncError);
        out.writeOptionalString(lastAccessControlSyncScheduledAt);
        out.writeOptionalEnum(lastAccessControlSyncStatus);
        out.writeOptionalLong(lastDeletedDocumentCount);
        out.writeOptionalString(lastIncrementalSyncScheduledAt);
        out.writeOptionalLong(lastIndexedDocumentCount);
        out.writeOptionalString(lastSeen);
        out.writeOptionalString(lastSyncError);
        out.writeOptionalString(lastSyncScheduledAt);
        out.writeOptionalEnum(lastSyncStatus);
        out.writeOptionalString(lastSynced);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConnectorSyncInfo that = (ConnectorSyncInfo) o;
        return Objects.equals(lastAccessControlSyncError, that.lastAccessControlSyncError)
            && Objects.equals(lastAccessControlSyncScheduledAt, that.lastAccessControlSyncScheduledAt)
            && lastAccessControlSyncStatus == that.lastAccessControlSyncStatus
            && Objects.equals(lastDeletedDocumentCount, that.lastDeletedDocumentCount)
            && Objects.equals(lastIncrementalSyncScheduledAt, that.lastIncrementalSyncScheduledAt)
            && Objects.equals(lastIndexedDocumentCount, that.lastIndexedDocumentCount)
            && Objects.equals(lastSeen, that.lastSeen)
            && Objects.equals(lastSyncError, that.lastSyncError)
            && Objects.equals(lastSyncScheduledAt, that.lastSyncScheduledAt)
            && lastSyncStatus == that.lastSyncStatus
            && Objects.equals(lastSynced, that.lastSynced);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            lastAccessControlSyncError,
            lastAccessControlSyncScheduledAt,
            lastAccessControlSyncStatus,
            lastDeletedDocumentCount,
            lastIncrementalSyncScheduledAt,
            lastIndexedDocumentCount,
            lastSeen,
            lastSyncError,
            lastSyncScheduledAt,
            lastSyncStatus,
            lastSynced
        );
    }

    public static class Builder {

        private String lastAccessControlSyncError;
        private String lastAccessControlSyncScheduledAt;
        private ConnectorSyncStatus lastAccessControlSyncStatus;
        private Long lastDeletedDocumentCount;
        private String lastIncrementalSyncScheduledAt;
        private Long lastIndexedDocumentCount;
        private String lastSeen;
        private String lastSyncError;
        private String lastSyncScheduledAt;
        private ConnectorSyncStatus lastSyncStatus;
        private String lastSynced;

        public Builder setLastAccessControlSyncError(String lastAccessControlSyncError) {
            this.lastAccessControlSyncError = lastAccessControlSyncError;
            return this;
        }

        public Builder setLastAccessControlSyncScheduledAt(String lastAccessControlSyncScheduledAt) {
            this.lastAccessControlSyncScheduledAt = lastAccessControlSyncScheduledAt;
            return this;
        }

        public Builder setLastAccessControlSyncStatus(ConnectorSyncStatus lastAccessControlSyncStatus) {
            this.lastAccessControlSyncStatus = lastAccessControlSyncStatus;
            return this;
        }

        public Builder setLastDeletedDocumentCount(Long lastDeletedDocumentCount) {
            this.lastDeletedDocumentCount = lastDeletedDocumentCount;
            return this;
        }

        public Builder setLastIncrementalSyncScheduledAt(String lastIncrementalSyncScheduledAt) {
            this.lastIncrementalSyncScheduledAt = lastIncrementalSyncScheduledAt;
            return this;
        }

        public Builder setLastIndexedDocumentCount(Long lastIndexedDocumentCount) {
            this.lastIndexedDocumentCount = lastIndexedDocumentCount;
            return this;
        }

        public Builder setLastSeen(String lastSeen) {
            this.lastSeen = lastSeen;
            return this;
        }

        public Builder setLastSyncError(String lastSyncError) {
            this.lastSyncError = lastSyncError;
            return this;
        }

        public Builder setLastSyncScheduledAt(String lastSyncScheduledAt) {
            this.lastSyncScheduledAt = lastSyncScheduledAt;
            return this;
        }

        public Builder setLastSyncStatus(ConnectorSyncStatus lastSyncStatus) {
            this.lastSyncStatus = lastSyncStatus;
            return this;
        }

        public Builder setLastSynced(String lastSynced) {
            this.lastSynced = lastSynced;
            return this;
        }

        public ConnectorSyncInfo build() {
            return new ConnectorSyncInfo(
                lastAccessControlSyncError,
                lastAccessControlSyncScheduledAt,
                lastAccessControlSyncStatus,
                lastDeletedDocumentCount,
                lastIncrementalSyncScheduledAt,
                lastIndexedDocumentCount,
                lastSeen,
                lastSyncError,
                lastSyncScheduledAt,
                lastSyncStatus,
                lastSynced
            );
        }
    }
}
