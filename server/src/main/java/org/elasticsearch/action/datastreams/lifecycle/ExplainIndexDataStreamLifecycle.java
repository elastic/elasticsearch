/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.datastreams.lifecycle;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.admin.indices.rollover.RolloverConfiguration;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetention;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.TransportVersions.V_8_12_0;

/**
 * Encapsulates the information that describes an index from its data stream lifecycle perspective.
 */
public class ExplainIndexDataStreamLifecycle implements Writeable, ToXContentObject {
    private static final ParseField INDEX_FIELD = new ParseField("index");
    private static final ParseField MANAGED_BY_LIFECYCLE_FIELD = new ParseField("managed_by_lifecycle");
    private static final ParseField INDEX_CREATION_DATE_MILLIS_FIELD = new ParseField("index_creation_date_millis");
    private static final ParseField INDEX_CREATION_DATE_FIELD = new ParseField("index_creation_date");
    private static final ParseField ROLLOVER_DATE_MILLIS_FIELD = new ParseField("rollover_date_millis");
    private static final ParseField ROLLOVER_DATE_FIELD = new ParseField("rollover_date");
    private static final ParseField TIME_SINCE_INDEX_CREATION_FIELD = new ParseField("time_since_index_creation");
    private static final ParseField TIME_SINCE_ROLLOVER_FIELD = new ParseField("time_since_rollover");
    private static final ParseField GENERATION_TIME = new ParseField("generation_time");
    private static final ParseField LIFECYCLE_FIELD = new ParseField("lifecycle");
    private static final ParseField ERROR_FIELD = new ParseField("error");

    private final String index;
    private final boolean managedByLifecycle;
    private final boolean isInternalDataStream;
    @Nullable
    private final Long indexCreationDate;
    @Nullable
    private final Long rolloverDate;
    @Nullable
    private final Long generationDateMillis;
    @Nullable
    private final DataStreamLifecycle lifecycle;
    @Nullable
    private final ErrorEntry error;
    private Supplier<Long> nowSupplier = System::currentTimeMillis;

    public ExplainIndexDataStreamLifecycle(
        String index,
        boolean managedByLifecycle,
        boolean isInternalDataStream,
        @Nullable Long indexCreationDate,
        @Nullable Long rolloverDate,
        @Nullable TimeValue generationDate,
        @Nullable DataStreamLifecycle lifecycle,
        @Nullable ErrorEntry error
    ) {
        this.index = index;
        this.managedByLifecycle = managedByLifecycle;
        this.isInternalDataStream = isInternalDataStream;
        this.indexCreationDate = indexCreationDate;
        this.rolloverDate = rolloverDate;
        this.generationDateMillis = generationDate == null ? null : generationDate.millis();
        this.lifecycle = lifecycle;
        this.error = error;
    }

    public ExplainIndexDataStreamLifecycle(StreamInput in) throws IOException {
        this.index = in.readString();
        this.managedByLifecycle = in.readBoolean();
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
            this.isInternalDataStream = in.readBoolean();
        } else {
            this.isInternalDataStream = false;
        }
        if (managedByLifecycle) {
            this.indexCreationDate = in.readOptionalLong();
            this.rolloverDate = in.readOptionalLong();
            this.generationDateMillis = in.readOptionalLong();
            this.lifecycle = in.readOptionalWriteable(DataStreamLifecycle::new);
            if (in.getTransportVersion().onOrAfter(V_8_12_0)) {
                this.error = in.readOptionalWriteable(ErrorEntry::new);
            } else {
                String bwcErrorMessage = in.readOptionalString();
                this.error = new ErrorEntry(-1L, bwcErrorMessage, -1L, -1);
            }
        } else {
            this.indexCreationDate = null;
            this.rolloverDate = null;
            this.generationDateMillis = null;
            this.lifecycle = null;
            this.error = null;
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return toXContent(builder, params, null, null);
    }

    public XContentBuilder toXContent(
        XContentBuilder builder,
        Params params,
        @Nullable RolloverConfiguration rolloverConfiguration,
        @Nullable DataStreamGlobalRetention globalRetention
    ) throws IOException {
        builder.startObject();
        builder.field(INDEX_FIELD.getPreferredName(), index);
        builder.field(MANAGED_BY_LIFECYCLE_FIELD.getPreferredName(), managedByLifecycle);
        if (managedByLifecycle) {
            if (indexCreationDate != null) {
                builder.timestampFieldsFromUnixEpochMillis(
                    INDEX_CREATION_DATE_MILLIS_FIELD.getPreferredName(),
                    INDEX_CREATION_DATE_FIELD.getPreferredName(),
                    indexCreationDate
                );
                builder.field(
                    TIME_SINCE_INDEX_CREATION_FIELD.getPreferredName(),
                    getTimeSinceIndexCreation(nowSupplier).toHumanReadableString(2)
                );
            }
            if (rolloverDate != null) {
                builder.timestampFieldsFromUnixEpochMillis(
                    ROLLOVER_DATE_MILLIS_FIELD.getPreferredName(),
                    ROLLOVER_DATE_FIELD.getPreferredName(),
                    rolloverDate
                );
                builder.field(TIME_SINCE_ROLLOVER_FIELD.getPreferredName(), getTimeSinceRollover(nowSupplier).toHumanReadableString(2));
            }
            if (generationDateMillis != null) {
                builder.field(GENERATION_TIME.getPreferredName(), getGenerationTime(nowSupplier).toHumanReadableString(2));
            }
            if (this.lifecycle != null) {
                builder.field(LIFECYCLE_FIELD.getPreferredName());
                lifecycle.toXContent(builder, params, rolloverConfiguration, globalRetention, isInternalDataStream);
            }
            if (this.error != null) {
                if (error.firstOccurrenceTimestamp() != -1L && error.recordedTimestamp() != -1L && error.retryCount() != -1) {
                    builder.field(ERROR_FIELD.getPreferredName(), error);
                } else {
                    // bwc for error field being a string
                    builder.field(ERROR_FIELD.getPreferredName(), error.error());
                }
            }
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeBoolean(managedByLifecycle);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
            out.writeBoolean(isInternalDataStream);
        }
        if (managedByLifecycle) {
            out.writeOptionalLong(indexCreationDate);
            out.writeOptionalLong(rolloverDate);
            out.writeOptionalLong(generationDateMillis);
            out.writeOptionalWriteable(lifecycle);
            if (out.getTransportVersion().onOrAfter(V_8_12_0)) {
                out.writeOptionalWriteable(error);
            } else {
                String errorMessage = error != null ? error.error() : null;
                out.writeOptionalString(errorMessage);
            }
        }
    }

    /**
     * Calculates the time since this index started progressing towards the remaining of its lifecycle past rollover.
     * Every index will either have to wait to be rolled over before progressing towards its retention part of its lifecycle,
     * or be added to the datastream manually.
     * If the index is the write index this will return null.
     */
    @Nullable
    public TimeValue getGenerationTime(Supplier<Long> now) {
        if (generationDateMillis == null) {
            return null;
        }
        return TimeValue.timeValueMillis(Math.max(0L, now.get() - generationDateMillis));
    }

    /**
     * Calculates the time lapsed since the index was created.
     * It can be null as we don't serialise the index creation field for un-managed indices.
     */
    @Nullable
    public TimeValue getTimeSinceIndexCreation(Supplier<Long> now) {
        if (indexCreationDate == null) {
            // unmanaged index
            return null;
        }
        return TimeValue.timeValueMillis(Math.max(0L, now.get() - indexCreationDate));
    }

    /**
     * Calculates the time lapsed since the index was rolled over.
     * It can be null if the index was not rolled over or for un-managed indecs as we don't serialise the rollover data field.
     */
    @Nullable
    public TimeValue getTimeSinceRollover(Supplier<Long> now) {
        if (rolloverDate == null) {
            return null;
        }
        return TimeValue.timeValueMillis(Math.max(0L, now.get() - rolloverDate));
    }

    public String getIndex() {
        return index;
    }

    public boolean isManagedByLifecycle() {
        return managedByLifecycle;
    }

    public Long getIndexCreationDate() {
        return indexCreationDate;
    }

    public Long getRolloverDate() {
        return rolloverDate;
    }

    public DataStreamLifecycle getLifecycle() {
        return lifecycle;
    }

    public ErrorEntry getError() {
        return error;
    }

    // public for testing purposes only
    public void setNowSupplier(Supplier<Long> nowSupplier) {
        this.nowSupplier = nowSupplier;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ExplainIndexDataStreamLifecycle that = (ExplainIndexDataStreamLifecycle) o;
        return managedByLifecycle == that.managedByLifecycle
            && Objects.equals(index, that.index)
            && Objects.equals(indexCreationDate, that.indexCreationDate)
            && Objects.equals(rolloverDate, that.rolloverDate)
            && Objects.equals(lifecycle, that.lifecycle)
            && Objects.equals(error, that.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, managedByLifecycle, indexCreationDate, rolloverDate, lifecycle, error);
    }
}
