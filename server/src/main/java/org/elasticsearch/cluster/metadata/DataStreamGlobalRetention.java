/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;

/**
 * A cluster state entry that contains global retention settings that are configurable by the user. These settings include:
 * - default retention, applied on any data stream managed by DSL that does not have an explicit retention defined
 * - max retention, applied on every data stream managed by DSL
 */
public final class DataStreamGlobalRetention extends AbstractNamedDiffable<ClusterState.Custom> implements ClusterState.Custom {

    public static final String TYPE = "data-stream-global-retention";

    public static final NodeFeature GLOBAL_RETENTION = new NodeFeature("data_stream.lifecycle.global_retention");

    public static final ParseField DEFAULT_RETENTION_FIELD = new ParseField("default_retention");
    public static final ParseField MAX_RETENTION_FIELD = new ParseField("max_retention");

    public static final DataStreamGlobalRetention EMPTY = new DataStreamGlobalRetention(null, null);
    public static final TimeValue MIN_RETENTION_VALUE = TimeValue.timeValueSeconds(10);

    @Nullable
    private final TimeValue defaultRetention;
    @Nullable
    private final TimeValue maxRetention;

    /**
     * @param defaultRetention the default retention or null if it's undefined
     * @param maxRetention the max retention or null if it's undefined
     * @throws IllegalArgumentException when the default retention is greater than the max retention.
     */
    public DataStreamGlobalRetention(TimeValue defaultRetention, TimeValue maxRetention) {
        if (defaultRetention != null && maxRetention != null && defaultRetention.getMillis() > maxRetention.getMillis()) {
            throw new IllegalArgumentException(
                "Default global retention ["
                    + defaultRetention.getStringRep()
                    + "] cannot be greater than the max global retention ["
                    + maxRetention.getStringRep()
                    + "]."
            );
        }
        if (validateRetentionValue(defaultRetention) == false || validateRetentionValue(maxRetention) == false) {
            throw new IllegalArgumentException("Global retention values should be greater than " + MIN_RETENTION_VALUE.getStringRep());
        }
        this.defaultRetention = defaultRetention;
        this.maxRetention = maxRetention;
    }

    private boolean validateRetentionValue(@Nullable TimeValue retention) {
        return retention == null || retention.getMillis() >= MIN_RETENTION_VALUE.getMillis();
    }

    public static DataStreamGlobalRetention read(StreamInput in) throws IOException {
        return new DataStreamGlobalRetention(in.readOptionalTimeValue(), in.readOptionalTimeValue());
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_14_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalTimeValue(defaultRetention);
        out.writeOptionalTimeValue(maxRetention);
    }

    public static NamedDiff<ClusterState.Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(ClusterState.Custom.class, TYPE, in);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
        return Iterators.single(this::toXContentFragment);
    }

    /**
     * Adds to the XContentBuilder the two fields when they are not null.
     */
    public XContentBuilder toXContentFragment(XContentBuilder builder, ToXContent.Params params) throws IOException {
        if (defaultRetention != null) {
            builder.field(DEFAULT_RETENTION_FIELD.getPreferredName(), defaultRetention.getStringRep());
        }
        if (maxRetention != null) {
            builder.field(MAX_RETENTION_FIELD.getPreferredName(), maxRetention.getStringRep());
        }
        return builder;
    }

    /**
     * Returns the metadata found in the cluster state or null. When trying to retrieve the effective global retention,
     * prefer to use the {@link DataStreamGlobalRetentionResolver#resolve(ClusterState)} because it takes into account
     * the factory retention settings as well. Only use this, if you only want to know the global retention settings
     * stored in the cluster metadata.
     */
    @Nullable
    public static DataStreamGlobalRetention getFromClusterState(ClusterState clusterState) {
        return clusterState.custom(DataStreamGlobalRetention.TYPE);
    }

    @Nullable
    public TimeValue getDefaultRetention() {
        return defaultRetention;
    }

    @Nullable
    public TimeValue getMaxRetention() {
        return maxRetention;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataStreamGlobalRetention that = (DataStreamGlobalRetention) o;
        return Objects.equals(defaultRetention, that.defaultRetention) && Objects.equals(maxRetention, that.maxRetention);
    }

    @Override
    public int hashCode() {
        return Objects.hash(defaultRetention, maxRetention);
    }

    @Override
    public String toString() {
        return "DataStreamGlobalRetention{"
            + "defaultRetention="
            + (defaultRetention == null ? "null" : defaultRetention.getStringRep())
            + ", maxRetention="
            + (maxRetention == null ? "null" : maxRetention.getStringRep())
            + '}';
    }
}
