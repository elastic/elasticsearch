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
import java.time.Instant;
import java.util.Objects;

public class ConnectorSyncInfo implements Writeable, ToXContentFragment {
    @Nullable
    private final String lastAccessControlSyncError;
    @Nullable
    private final Instant lastAccessControlSyncScheduledAt;
    @Nullable
    private final ConnectorSyncStatus lastAccessControlSyncStatus;
    @Nullable
    private final Long lastDeletedDocumentCount;
    @Nullable
    private final Instant lastIncrementalSyncScheduledAt;
    @Nullable
    private final Long lastIndexedDocumentCount;
    @Nullable
    private final String lastSyncError;
    @Nullable
    private final Instant lastSyncScheduledAt;
    @Nullable
    private final ConnectorSyncStatus lastSyncStatus;
    @Nullable
    private final Instant lastSynced;

    /**
     * @param lastAccessControlSyncError      The last error message related to access control sync, if any.
     * @param lastAccessControlSyncScheduledAt The timestamp when the last access control sync was scheduled.
     * @param lastAccessControlSyncStatus     The status of the last access control sync.
     * @param lastDeletedDocumentCount        The count of documents last deleted during sync.
     * @param lastIncrementalSyncScheduledAt  The timestamp when the last incremental sync was scheduled.
     * @param lastIndexedDocumentCount        The count of documents last indexed during sync.
     * @param lastSyncError                   The last error message encountered during sync, if any.
     * @param lastSyncScheduledAt             The timestamp when the last sync was scheduled.
     * @param lastSyncStatus                  The status of the last sync.
     * @param lastSynced                      The timestamp when the connector was last successfully synchronized.
     */
    private ConnectorSyncInfo(
        String lastAccessControlSyncError,
        Instant lastAccessControlSyncScheduledAt,
        ConnectorSyncStatus lastAccessControlSyncStatus,
        Long lastDeletedDocumentCount,
        Instant lastIncrementalSyncScheduledAt,
        Long lastIndexedDocumentCount,
        String lastSyncError,
        Instant lastSyncScheduledAt,
        ConnectorSyncStatus lastSyncStatus,
        Instant lastSynced
    ) {
        this.lastAccessControlSyncError = lastAccessControlSyncError;
        this.lastAccessControlSyncScheduledAt = lastAccessControlSyncScheduledAt;
        this.lastAccessControlSyncStatus = lastAccessControlSyncStatus;
        this.lastDeletedDocumentCount = lastDeletedDocumentCount;
        this.lastIncrementalSyncScheduledAt = lastIncrementalSyncScheduledAt;
        this.lastIndexedDocumentCount = lastIndexedDocumentCount;
        this.lastSyncError = lastSyncError;
        this.lastSyncScheduledAt = lastSyncScheduledAt;
        this.lastSyncStatus = lastSyncStatus;
        this.lastSynced = lastSynced;
    }

    public ConnectorSyncInfo(StreamInput in) throws IOException {
        this.lastAccessControlSyncError = in.readOptionalString();
        this.lastAccessControlSyncScheduledAt = in.readOptionalInstant();
        this.lastAccessControlSyncStatus = in.readOptionalEnum(ConnectorSyncStatus.class);
        this.lastDeletedDocumentCount = in.readOptionalLong();
        this.lastIncrementalSyncScheduledAt = in.readOptionalInstant();
        this.lastIndexedDocumentCount = in.readOptionalLong();
        this.lastSyncError = in.readOptionalString();
        this.lastSyncScheduledAt = in.readOptionalInstant();
        this.lastSyncStatus = in.readOptionalEnum(ConnectorSyncStatus.class);
        this.lastSynced = in.readOptionalInstant();
    }

    public static final ParseField LAST_ACCESS_CONTROL_SYNC_ERROR = new ParseField("last_access_control_sync_error");
    public static final ParseField LAST_ACCESS_CONTROL_SYNC_STATUS_FIELD = new ParseField("last_access_control_sync_status");
    public static final ParseField LAST_ACCESS_CONTROL_SYNC_SCHEDULED_AT_FIELD = new ParseField("last_access_control_sync_scheduled_at");
    public static final ParseField LAST_DELETED_DOCUMENT_COUNT_FIELD = new ParseField("last_deleted_document_count");
    public static final ParseField LAST_INCREMENTAL_SYNC_SCHEDULED_AT_FIELD = new ParseField("last_incremental_sync_scheduled_at");
    public static final ParseField LAST_INDEXED_DOCUMENT_COUNT_FIELD = new ParseField("last_indexed_document_count");
    public static final ParseField LAST_SYNC_ERROR_FIELD = new ParseField("last_sync_error");
    public static final ParseField LAST_SYNC_SCHEDULED_AT_FIELD = new ParseField("last_sync_scheduled_at");
    public static final ParseField LAST_SYNC_STATUS_FIELD = new ParseField("last_sync_status");
    public static final ParseField LAST_SYNCED_FIELD = new ParseField("last_synced");

    public String getLastAccessControlSyncError() {
        return lastAccessControlSyncError;
    }

    public Instant getLastAccessControlSyncScheduledAt() {
        return lastAccessControlSyncScheduledAt;
    }

    public ConnectorSyncStatus getLastAccessControlSyncStatus() {
        return lastAccessControlSyncStatus;
    }

    public Long getLastDeletedDocumentCount() {
        return lastDeletedDocumentCount;
    }

    public Instant getLastIncrementalSyncScheduledAt() {
        return lastIncrementalSyncScheduledAt;
    }

    public Long getLastIndexedDocumentCount() {
        return lastIndexedDocumentCount;
    }

    public String getLastSyncError() {
        return lastSyncError;
    }

    public Instant getLastSyncScheduledAt() {
        return lastSyncScheduledAt;
    }

    public ConnectorSyncStatus getLastSyncStatus() {
        return lastSyncStatus;
    }

    public Instant getLastSynced() {
        return lastSynced;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (lastAccessControlSyncError != null) {
            builder.field(LAST_ACCESS_CONTROL_SYNC_ERROR.getPreferredName(), lastAccessControlSyncError);
        }
        if (lastAccessControlSyncStatus != null) {
            builder.field(LAST_ACCESS_CONTROL_SYNC_STATUS_FIELD.getPreferredName(), lastAccessControlSyncStatus);
        }
        if (lastAccessControlSyncScheduledAt != null) {
            builder.field(LAST_ACCESS_CONTROL_SYNC_SCHEDULED_AT_FIELD.getPreferredName(), lastAccessControlSyncScheduledAt);
        }
        if (lastDeletedDocumentCount != null) {
            builder.field(LAST_DELETED_DOCUMENT_COUNT_FIELD.getPreferredName(), lastDeletedDocumentCount);
        }
        if (lastIncrementalSyncScheduledAt != null) {
            builder.field(LAST_INCREMENTAL_SYNC_SCHEDULED_AT_FIELD.getPreferredName(), lastIncrementalSyncScheduledAt);
        }
        if (lastIndexedDocumentCount != null) {
            builder.field(LAST_INDEXED_DOCUMENT_COUNT_FIELD.getPreferredName(), lastIndexedDocumentCount);
        }
        if (lastSyncError != null) {
            builder.field(LAST_SYNC_ERROR_FIELD.getPreferredName(), lastSyncError);
        }
        if (lastSyncScheduledAt != null) {
            builder.field(LAST_SYNC_SCHEDULED_AT_FIELD.getPreferredName(), lastSyncScheduledAt);
        }
        if (lastSyncStatus != null) {
            builder.field(LAST_SYNC_STATUS_FIELD.getPreferredName(), lastSyncStatus);
        }
        if (lastSynced != null) {
            builder.field(LAST_SYNCED_FIELD.getPreferredName(), lastSynced);
        }
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(lastAccessControlSyncError);
        out.writeOptionalInstant(lastAccessControlSyncScheduledAt);
        out.writeOptionalEnum(lastAccessControlSyncStatus);
        out.writeOptionalLong(lastDeletedDocumentCount);
        out.writeOptionalInstant(lastIncrementalSyncScheduledAt);
        out.writeOptionalLong(lastIndexedDocumentCount);
        out.writeOptionalString(lastSyncError);
        out.writeOptionalInstant(lastSyncScheduledAt);
        out.writeOptionalEnum(lastSyncStatus);
        out.writeOptionalInstant(lastSynced);
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
            lastSyncError,
            lastSyncScheduledAt,
            lastSyncStatus,
            lastSynced
        );
    }

    public static class Builder {

        private String lastAccessControlSyncError;
        private Instant lastAccessControlSyncScheduledAt;
        private ConnectorSyncStatus lastAccessControlSyncStatus;
        private Long lastDeletedDocumentCount;
        private Instant lastIncrementalSyncScheduledAt;
        private Long lastIndexedDocumentCount;
        private String lastSyncError;
        private Instant lastSyncScheduledAt;
        private ConnectorSyncStatus lastSyncStatus;
        private Instant lastSynced;

        public Builder setLastAccessControlSyncError(String lastAccessControlSyncError) {
            this.lastAccessControlSyncError = lastAccessControlSyncError;
            return this;
        }

        public Builder setLastAccessControlSyncScheduledAt(Instant lastAccessControlSyncScheduledAt) {
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

        public Builder setLastIncrementalSyncScheduledAt(Instant lastIncrementalSyncScheduledAt) {
            this.lastIncrementalSyncScheduledAt = lastIncrementalSyncScheduledAt;
            return this;
        }

        public Builder setLastIndexedDocumentCount(Long lastIndexedDocumentCount) {
            this.lastIndexedDocumentCount = lastIndexedDocumentCount;
            return this;
        }

        public Builder setLastSyncError(String lastSyncError) {
            this.lastSyncError = lastSyncError;
            return this;
        }

        public Builder setLastSyncScheduledAt(Instant lastSyncScheduledAt) {
            this.lastSyncScheduledAt = lastSyncScheduledAt;
            return this;
        }

        public Builder setLastSyncStatus(ConnectorSyncStatus lastSyncStatus) {
            this.lastSyncStatus = lastSyncStatus;
            return this;
        }

        public Builder setLastSynced(Instant lastSynced) {
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
                lastSyncError,
                lastSyncScheduledAt,
                lastSyncStatus,
                lastSynced
            );
        }
    }
}
