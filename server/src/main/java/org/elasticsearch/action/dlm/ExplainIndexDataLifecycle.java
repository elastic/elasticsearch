/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.dlm;

import org.elasticsearch.action.admin.indices.rollover.RolloverConditions;
import org.elasticsearch.cluster.metadata.DataLifecycle;
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

/**
 * Encapsulates the information that describes an index from its DLM lifecycle perspective.
 */
public class ExplainIndexDataLifecycle implements Writeable, ToXContentObject {
    private static final ParseField INDEX_FIELD = new ParseField("index");
    private static final ParseField MANAGED_BY_DLM_FIELD = new ParseField("managed_by_dlm");
    private static final ParseField INDEX_CREATION_DATE_MILLIS_FIELD = new ParseField("index_creation_date_millis");
    private static final ParseField INDEX_CREATION_DATE_FIELD = new ParseField("index_creation_date");
    private static final ParseField ROLLOVER_DATE_MILLIS_FIELD = new ParseField("rollover_date_millis");
    private static final ParseField ROLLOVER_DATE_FIELD = new ParseField("rollover_date");
    private static final ParseField TIME_SINCE_INDEX_CREATION_FIELD = new ParseField("time_since_index_creation");
    private static final ParseField TIME_SINCE_ROLLOVER_FIELD = new ParseField("time_since_rollover");
    private static final ParseField AGE_FIELD = new ParseField("age");
    private static final ParseField LIFECYCLE_FIELD = new ParseField("lifecycle");
    private static final ParseField ERROR_FIELD = new ParseField("error");

    private final String index;
    private final boolean managedByDLM;
    @Nullable
    private final Long indexCreationDate;
    @Nullable
    private final Long rolloverDate;
    @Nullable
    private final DataLifecycle lifecycle;
    @Nullable
    private final String error;
    private Supplier<Long> nowSupplier = System::currentTimeMillis;

    public ExplainIndexDataLifecycle(
        String index,
        boolean managedByDLM,
        @Nullable Long indexCreationDate,
        @Nullable Long rolloverDate,
        @Nullable DataLifecycle lifecycle,
        @Nullable String error
    ) {
        this.index = index;
        this.managedByDLM = managedByDLM;
        this.indexCreationDate = indexCreationDate;
        this.rolloverDate = rolloverDate;
        this.lifecycle = lifecycle;
        this.error = error;
    }

    public ExplainIndexDataLifecycle(StreamInput in) throws IOException {
        this.index = in.readString();
        this.managedByDLM = in.readBoolean();
        if (managedByDLM) {
            this.indexCreationDate = in.readOptionalLong();
            this.rolloverDate = in.readOptionalLong();
            this.lifecycle = in.readOptionalWriteable(DataLifecycle::new);
            this.error = in.readOptionalString();
        } else {
            this.indexCreationDate = null;
            this.rolloverDate = null;
            this.lifecycle = null;
            this.error = null;
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return toXContent(builder, params, null);
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params, @Nullable RolloverConditions rolloverConditions)
        throws IOException {
        builder.startObject();
        builder.field(INDEX_FIELD.getPreferredName(), index);
        builder.field(MANAGED_BY_DLM_FIELD.getPreferredName(), managedByDLM);
        if (managedByDLM) {
            if (indexCreationDate != null) {
                builder.timeField(
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
                builder.timeField(ROLLOVER_DATE_MILLIS_FIELD.getPreferredName(), ROLLOVER_DATE_FIELD.getPreferredName(), rolloverDate);
                builder.field(TIME_SINCE_ROLLOVER_FIELD.getPreferredName(), getTimeSinceRollover(nowSupplier).toHumanReadableString(2));
            }
            if (this.lifecycle != null) {
                builder.field(LIFECYCLE_FIELD.getPreferredName());
                lifecycle.toXContent(builder, params, rolloverConditions);
            }
            builder.field(AGE_FIELD.getPreferredName(), getAge(nowSupplier).toHumanReadableString(2));
            if (this.error != null) {
                builder.field(ERROR_FIELD.getPreferredName(), error);
            }
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeBoolean(managedByDLM);
        if (managedByDLM) {
            out.writeOptionalLong(indexCreationDate);
            out.writeOptionalLong(rolloverDate);
            out.writeOptionalWriteable(lifecycle);
            out.writeOptionalString(error);
        }
    }

    @Nullable
    public TimeValue getAge(Supplier<Long> now) {
        Long lifecycleDate = rolloverDate != null ? rolloverDate : indexCreationDate;
        if (lifecycleDate == null) {
            return null;
        }
        return TimeValue.timeValueMillis(Math.max(0L, now.get() - lifecycleDate));
    }

    @Nullable
    public TimeValue getTimeSinceIndexCreation(Supplier<Long> now) {
        if (indexCreationDate == null) {
            // unmanaged index
            return null;
        }
        return TimeValue.timeValueMillis(Math.max(0L, now.get() - indexCreationDate));
    }

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

    public boolean isManagedByDLM() {
        return managedByDLM;
    }

    public Long getIndexCreationDate() {
        return indexCreationDate;
    }

    public Long getRolloverDate() {
        return rolloverDate;
    }

    public DataLifecycle getLifecycle() {
        return lifecycle;
    }

    public String getError() {
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
        ExplainIndexDataLifecycle that = (ExplainIndexDataLifecycle) o;
        return managedByDLM == that.managedByDLM
            && Objects.equals(index, that.index)
            && Objects.equals(indexCreationDate, that.indexCreationDate)
            && Objects.equals(rolloverDate, that.rolloverDate)
            && Objects.equals(lifecycle, that.lifecycle)
            && Objects.equals(error, that.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, managedByDLM, indexCreationDate, rolloverDate, lifecycle, error);
    }
}
