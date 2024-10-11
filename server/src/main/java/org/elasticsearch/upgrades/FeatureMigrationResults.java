/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.upgrades;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Holds the results of the most recent attempt to migrate system indices. Updated by {@link SystemIndexMigrator} as it finishes each
 * feature, or fails.
 */
public class FeatureMigrationResults implements Metadata.Custom {
    public static final String TYPE = "system_index_migration";
    public static final TransportVersion MIGRATION_ADDED_VERSION = TransportVersions.V_8_0_0;

    static final ParseField RESULTS_FIELD = new ParseField("results");

    private final Map<String, SingleFeatureMigrationResult> featureStatuses;

    public FeatureMigrationResults(Map<String, SingleFeatureMigrationResult> featureStatuses) {
        this.featureStatuses = Objects.requireNonNullElse(featureStatuses, new HashMap<>());
    }

    public FeatureMigrationResults(StreamInput in) throws IOException {
        this.featureStatuses = in.readMap(SingleFeatureMigrationResult::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(featureStatuses, StreamOutput::writeWriteable);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return ChunkedToXContent.builder(params).xContentObjectFields(RESULTS_FIELD.getPreferredName(), featureStatuses);
    }

    /**
     * Gets a map of feature name to that feature's status. Only contains features which have either been migrated successfully or
     * failed to migrate.
     * @return An unmodifiable map of feature names to migration statuses.
     */
    public Map<String, SingleFeatureMigrationResult> getFeatureStatuses() {
        return Collections.unmodifiableMap(featureStatuses);
    }

    /**
     * Convenience method for updating the results of a migration run. Produces a new {@link FeatureMigrationResults} updated with the
     * given status for the given feature name.
     * @param featureName The feature name to update. If this feature name is already present, its status will be overwritten.
     * @param status The status that should be associated with the given {@code featureName}.
     * @return A new {@link FeatureMigrationResults} with the given status associated with the given feature name. Other entries in the
     *         map are unchanged.
     */
    public FeatureMigrationResults withResult(String featureName, SingleFeatureMigrationResult status) {
        Map<String, SingleFeatureMigrationResult> newMap = new HashMap<>(featureStatuses);
        newMap.put(featureName, status);
        return new FeatureMigrationResults(newMap);
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.ALL_CONTEXTS;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return MIGRATION_ADDED_VERSION;
    }

    @Override
    public String toString() {
        return "SystemIndexMigrationResult{" + "featureStatuses=" + featureStatuses + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o instanceof FeatureMigrationResults) == false) return false;
        FeatureMigrationResults that = (FeatureMigrationResults) o;
        return featureStatuses.equals(that.featureStatuses);
    }

    @Override
    public int hashCode() {
        return Objects.hash(featureStatuses);
    }

    @Override
    public Diff<Metadata.Custom> diff(Metadata.Custom previousState) {
        return new ResultsDiff((FeatureMigrationResults) previousState, this);
    }

    public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
        return new ResultsDiff(in);
    }

    public static class ResultsDiff implements NamedDiff<Metadata.Custom> {
        private final Diff<Map<String, SingleFeatureMigrationResult>> resultsDiff;

        public ResultsDiff(FeatureMigrationResults before, FeatureMigrationResults after) {
            this.resultsDiff = DiffableUtils.diff(before.featureStatuses, after.featureStatuses, DiffableUtils.getStringKeySerializer());
        }

        public ResultsDiff(StreamInput in) throws IOException {
            this.resultsDiff = DiffableUtils.readJdkMapDiff(
                in,
                DiffableUtils.getStringKeySerializer(),
                SingleFeatureMigrationResult::new,
                ResultsDiff::readDiffFrom
            );
        }

        @Override
        public Metadata.Custom apply(Metadata.Custom part) {
            TreeMap<String, SingleFeatureMigrationResult> newResults = new TreeMap<>(
                resultsDiff.apply(((FeatureMigrationResults) part).featureStatuses)
            );
            return new FeatureMigrationResults(newResults);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            resultsDiff.writeTo(out);
        }

        static Diff<SingleFeatureMigrationResult> readDiffFrom(StreamInput in) throws IOException {
            return SimpleDiffable.readDiffFrom(SingleFeatureMigrationResult::new, in);
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return MIGRATION_ADDED_VERSION;
        }

    }

}
