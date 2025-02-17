/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

/**
 * Defined the basic functionality a data stream lifecycle configuration must provide. Currently, it requires the following configurations:
 * - enabled
 * - data retention
 */
public interface BasicDataStreamLifecycle {

    // The following XContent params are used to enrich the DataStreamLifecycle json with effective retention information
    // This should be set only when the lifecycle is used in a response to the user and NEVER when we expect the json to
    // be deserialized.
    String INCLUDE_EFFECTIVE_RETENTION_PARAM_NAME = "include_effective_retention";
    Map<String, String> INCLUDE_EFFECTIVE_RETENTION_PARAMS = Map.of(DataStreamLifecycle.INCLUDE_EFFECTIVE_RETENTION_PARAM_NAME, "true");

    Tuple<TimeValue, RetentionSource> INFINITE_RETENTION = Tuple.tuple(null, RetentionSource.DATA_STREAM_CONFIGURATION);

    ParseField DATA_RETENTION_FIELD = new ParseField("data_retention");
    ParseField EFFECTIVE_RETENTION_FIELD = new ParseField("effective_retention");
    ParseField RETENTION_SOURCE_FIELD = new ParseField("retention_determined_by");

    /**
     * The least amount of time data should be kept by elasticsearch. The effective retention is a function with three parameters,
     * the {@link BasicDataStreamLifecycle#dataRetention}, the global retention and whether this lifecycle is associated with an internal
     * data stream.
     *
     * @param globalRetention      The global retention, or null if global retention does not exist.
     * @param isInternalDataStream A flag denoting if this lifecycle is associated with an internal data stream or not
     * @return the time period or null, null represents that data should never be deleted.
     */
    @Nullable
    default TimeValue getEffectiveDataRetention(@Nullable DataStreamGlobalRetention globalRetention, boolean isInternalDataStream) {
        return getEffectiveDataRetentionWithSource(globalRetention, isInternalDataStream).v1();
    }

    /**
     * The least amount of time data should be kept by elasticsearch.. The effective retention is a function with three parameters,
     * the {@link BasicDataStreamLifecycle#dataRetention}, the global retention and whether this lifecycle is associated with an internal
     * data stream.
     *
     * @param globalRetention      The global retention, or null if global retention does not exist.
     * @param isInternalDataStream A flag denoting if this lifecycle is associated with an internal data stream or not
     * @return A tuple containing the time period or null as v1 (where null represents that data should never be deleted), and the non-null
     * retention source as v2.
     */
    default Tuple<TimeValue, RetentionSource> getEffectiveDataRetentionWithSource(
        @Nullable DataStreamGlobalRetention globalRetention,
        boolean isInternalDataStream
    ) {
        // If lifecycle is disabled there is no effective retention
        if (isEnabled() == false) {
            return INFINITE_RETENTION;
        }

        TimeValue dataRetention = dataRetention();
        if (globalRetention == null || isInternalDataStream) {
            return Tuple.tuple(dataRetention, RetentionSource.DATA_STREAM_CONFIGURATION);
        }
        if (dataRetention == null) {
            return globalRetention.defaultRetention() != null
                ? Tuple.tuple(globalRetention.defaultRetention(), RetentionSource.DEFAULT_GLOBAL_RETENTION)
                : Tuple.tuple(globalRetention.maxRetention(), RetentionSource.MAX_GLOBAL_RETENTION);
        }
        if (globalRetention.maxRetention() != null && globalRetention.maxRetention().getMillis() < dataRetention.getMillis()) {
            return Tuple.tuple(globalRetention.maxRetention(), RetentionSource.MAX_GLOBAL_RETENTION);
        } else {
            return Tuple.tuple(dataRetention, RetentionSource.DATA_STREAM_CONFIGURATION);
        }
    }

    /**
     * This method checks if the effective retention is matching what the user has configured; if the effective retention
     * does not match then it adds a warning informing the user about the effective retention and the source.
     */
    default void addWarningHeaderIfDataRetentionNotEffective(
        @Nullable DataStreamGlobalRetention globalRetention,
        boolean isInternalDataStream
    ) {
        if (globalRetention == null || isInternalDataStream) {
            return;
        }
        Tuple<TimeValue, RetentionSource> effectiveDataRetentionWithSource = getEffectiveDataRetentionWithSource(
            globalRetention,
            isInternalDataStream
        );
        if (effectiveDataRetentionWithSource.v1() == null) {
            return;
        }
        var dataRetention = dataRetention();
        String effectiveRetentionStringRep = effectiveDataRetentionWithSource.v1().getStringRep();
        switch (effectiveDataRetentionWithSource.v2()) {
            case DEFAULT_GLOBAL_RETENTION -> HeaderWarning.addWarning(
                "Not providing a retention is not allowed for this project. The default retention of ["
                    + effectiveRetentionStringRep
                    + "] will be applied."
            );
            case MAX_GLOBAL_RETENTION -> {
                String retentionProvidedPart = dataRetention == null
                    ? "Not providing a retention is not allowed for this project."
                    : "The retention provided ["
                        + dataRetention.getStringRep()
                        + "] is exceeding the max allowed data retention of this project ["
                        + effectiveRetentionStringRep
                        + "].";
                HeaderWarning.addWarning(
                    retentionProvidedPart + " The max retention of [" + effectiveRetentionStringRep + "] will be applied."
                );
            }
            case DATA_STREAM_CONFIGURATION -> {
            }
        }
    }

    default XContentBuilder retentionToXContentFragment(
        XContentBuilder builder,
        ToXContent.Params params,
        @Nullable DataStreamGlobalRetention globalRetention,
        boolean isInternalDataStream
    ) throws IOException {
        TimeValue dataRetention = dataRetention();
        if (dataRetention != null) {
            builder.field(DATA_RETENTION_FIELD.getPreferredName(), dataRetention.getStringRep());
        }
        Tuple<TimeValue, RetentionSource> effectiveDataRetentionWithSource = getEffectiveDataRetentionWithSource(
            globalRetention,
            isInternalDataStream
        );
        if (params.paramAsBoolean(INCLUDE_EFFECTIVE_RETENTION_PARAM_NAME, false)) {
            if (effectiveDataRetentionWithSource.v1() != null) {
                builder.field(EFFECTIVE_RETENTION_FIELD.getPreferredName(), effectiveDataRetentionWithSource.v1().getStringRep());
                builder.field(RETENTION_SOURCE_FIELD.getPreferredName(), effectiveDataRetentionWithSource.v2().displayName());
            }
        }
        return builder;
    }

    /**
     * Adds a retention param to signal that this serialisation should include the effective retention metadata.
     *
     * @param params the XContent params to be extended with the new flag
     * @return XContent params with `include_effective_retention` set to true. If the flag exists it will override it.
     */
    static ToXContent.Params addEffectiveRetentionParams(ToXContent.Params params) {
        return new ToXContent.DelegatingMapParams(INCLUDE_EFFECTIVE_RETENTION_PARAMS, params);
    }

    @Nullable
    TimeValue dataRetention();

    @Nullable
    boolean isEnabled();

    /**
     * This enum represents all configuration sources that can influence the retention of a data stream.
     */
    enum RetentionSource {
        DATA_STREAM_CONFIGURATION,
        DEFAULT_GLOBAL_RETENTION,
        MAX_GLOBAL_RETENTION;

        public String displayName() {
            return this.toString().toLowerCase(Locale.ROOT);
        }
    }
}
