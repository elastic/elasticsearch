/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.migration;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A response showing whether system features need to be migrated and, feature by feature, which
 * indices need to be migrated.
 */
public class GetFeatureMigrationStatusResponse extends ActionResponse implements ToXContentObject {

    private final List<FeatureMigrationStatus> featureMigrationStatuses;
    private final MigrationStatus migrationStatus;

    /**
     * @param statuses A list of feature statuses
     * @param migrationStatus Whether system features need to be migrated
     */
    public GetFeatureMigrationStatusResponse(List<FeatureMigrationStatus> statuses, MigrationStatus migrationStatus) {
        this.featureMigrationStatuses = Objects.nonNull(statuses) ? statuses : Collections.emptyList();
        this.migrationStatus = migrationStatus;
    }

    /**
     * @param in A stream input for a serialized response object
     * @throws IOException if we can't deserialize the object
     */
    public GetFeatureMigrationStatusResponse(StreamInput in) throws IOException {
        super(in);
        this.featureMigrationStatuses = in.readList(FeatureMigrationStatus::new);
        this.migrationStatus = in.readEnum(MigrationStatus.class);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray("features");
        for (FeatureMigrationStatus featureMigrationStatus : featureMigrationStatuses) {
            builder.value(featureMigrationStatus);
        }
        builder.endArray();
        builder.field("migration_status", migrationStatus);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(this.featureMigrationStatuses);
        out.writeEnum(migrationStatus);
    }

    public List<FeatureMigrationStatus> getFeatureMigrationStatuses() {
        return featureMigrationStatuses;
    }

    public MigrationStatus getMigrationStatus() {
        return migrationStatus;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetFeatureMigrationStatusResponse that = (GetFeatureMigrationStatusResponse) o;
        return Objects.equals(featureMigrationStatuses, that.featureMigrationStatuses)
            && Objects.equals(migrationStatus, that.migrationStatus);
    }

    @Override
    public int hashCode() {
        return Objects.hash(featureMigrationStatuses, migrationStatus);
    }

    @Override
    public String toString() {
        return "GetFeatureMigrationStatusResponse{" +
            "featureMigrationStatuses=" + featureMigrationStatuses +
            ", migrationStatus='" + migrationStatus + '\'' +
            '}';
    }

    public enum MigrationStatus {
        MIGRATION_NEEDED,
        NO_MIGRATION_NEEDED,
        IN_PROGRESS
    }

    /**
     * A class for a particular feature, showing whether it needs to be migrated and the earliest
     * Elasticsearch version used to create one of this feature's system indices.
     */
    public static class FeatureMigrationStatus implements Writeable, ToXContentObject {
        private final String featureName;
        private final Version minimumIndexVersion;
        private final MigrationStatus migrationStatus;
        private final List<IndexVersion> indexVersions;

        /**
         * @param featureName Name of the feature
         * @param minimumIndexVersion Earliest Elasticsearch version used to create a system index for this feature
         * @param migrationStatus Whether the feature needs to be migrated
         * @param indexVersions A list of this feature's concrete indices and the Elasticsearch version that created them
         */
        public FeatureMigrationStatus(String featureName, Version minimumIndexVersion,
                                      MigrationStatus migrationStatus, List<IndexVersion> indexVersions) {
            this.featureName = featureName;
            this.minimumIndexVersion = minimumIndexVersion;
            this.migrationStatus = migrationStatus;
            this.indexVersions = indexVersions;
        }

        /**
         * @param in A stream input for a serialized feature status object
         * @throws IOException if we can't deserialize the object
         */
        public FeatureMigrationStatus(StreamInput in) throws IOException {
            this.featureName = in.readString();
            this.minimumIndexVersion = Version.readVersion(in);
            this.migrationStatus = in.readEnum(MigrationStatus.class);
            this.indexVersions = in.readList(IndexVersion::new);
        }

        public String getFeatureName() {
            return this.featureName;
        }

        public Version getMinimumIndexVersion() {
            return this.minimumIndexVersion;
        }

        public MigrationStatus getMigrationStatus() {
            return this.migrationStatus;
        }

        public List<IndexVersion> getIndexVersions() {
            return this.indexVersions;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(this.featureName);
            Version.writeVersion(this.minimumIndexVersion, out);
            out.writeEnum(this.migrationStatus);
            out.writeList(this.indexVersions);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("feature_name", this.featureName);
            builder.field("minimum_index_version", this.minimumIndexVersion.toString());
            builder.field("migration_status", this.migrationStatus);
            builder.startArray("indices");
            for (IndexVersion version : this.indexVersions) {
                builder.value(version);
            }
            builder.endArray();
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FeatureMigrationStatus that = (FeatureMigrationStatus) o;
            return Objects.equals(featureName, that.featureName)
                && Objects.equals(minimumIndexVersion, that.minimumIndexVersion)
                && Objects.equals(migrationStatus, that.migrationStatus)
                && Objects.equals(indexVersions, that.indexVersions);
        }

        @Override
        public int hashCode() {
            return Objects.hash(featureName, minimumIndexVersion, migrationStatus, indexVersions);
        }

        @Override
        public String toString() {
            return "FeatureMigrationStatus{" +
                "featureName='" + featureName + '\'' +
                ", minimumIndexVersion='" + minimumIndexVersion + '\'' +
                ", migrationStatus='" + migrationStatus + '\'' +
                ", indexVersions=" + indexVersions +
                '}';
        }
    }

    /**
     * A data class that holds an index name and the version of Elasticsearch with which that index was created
     */
    public static class IndexVersion implements Writeable, ToXContentObject {
        private final String indexName;
        private final Version version;

        /**
         * @param indexName Name of the index
         * @param version Version of Elasticsearch that created the index
         */
        public IndexVersion(String indexName, Version version) {
            this.indexName = indexName;
            this.version = version;
        }

        /**
         * @param in A stream input for a serialized index version object
         * @throws IOException if we can't deserialize the object
         */
        public IndexVersion(StreamInput in) throws IOException {
            this.indexName = in.readString();
            this.version = Version.readVersion(in);
        }

        public String getIndexName() {
            return this.indexName;
        }

        public Version getVersion() {
            return this.version;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(this.indexName);
            Version.writeVersion(this.version, out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("index", this.indexName);
            builder.field("version", this.version.toString());
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IndexVersion that = (IndexVersion) o;
            return indexName.equals(that.indexName) && version.equals(that.version);
        }

        @Override
        public int hashCode() {
            return Objects.hash(indexName, version);
        }

        @Override
        public String toString() {
            return "IndexVersion{" +
                "indexName='" + indexName + '\'' +
                ", version='" + version + '\'' +
                '}';
        }
    }
}
