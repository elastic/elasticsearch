/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.system_indices.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A response showing whether system features need to be upgraded and, feature by feature, which
 * indices need to be upgraded.
 */
public class GetFeatureUpgradeStatusResponse extends ActionResponse implements ToXContentObject {

    private final List<FeatureUpgradeStatus> featureUpgradeStatuses;
    private final UpgradeStatus upgradeStatus;

    /**
     * @param statuses A list of feature statuses
     * @param upgradeStatus Whether system features need to be upgraded
     */
    public GetFeatureUpgradeStatusResponse(List<FeatureUpgradeStatus> statuses, UpgradeStatus upgradeStatus) {
        this.featureUpgradeStatuses = Objects.nonNull(statuses) ? statuses : Collections.emptyList();
        this.upgradeStatus = upgradeStatus;
    }

    /**
     * @param in A stream input for a serialized response object
     * @throws IOException if we can't deserialize the object
     */
    public GetFeatureUpgradeStatusResponse(StreamInput in) throws IOException {
        this.featureUpgradeStatuses = in.readCollectionAsImmutableList(FeatureUpgradeStatus::new);
        this.upgradeStatus = in.readEnum(UpgradeStatus.class);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray("features");
        for (FeatureUpgradeStatus featureUpgradeStatus : featureUpgradeStatuses) {
            builder.value(featureUpgradeStatus);
        }
        builder.endArray();
        builder.field("migration_status", upgradeStatus);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(this.featureUpgradeStatuses);
        out.writeEnum(upgradeStatus);
    }

    public List<FeatureUpgradeStatus> getFeatureUpgradeStatuses() {
        return featureUpgradeStatuses;
    }

    public UpgradeStatus getUpgradeStatus() {
        return upgradeStatus;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetFeatureUpgradeStatusResponse that = (GetFeatureUpgradeStatusResponse) o;
        return Objects.equals(featureUpgradeStatuses, that.featureUpgradeStatuses) && Objects.equals(upgradeStatus, that.upgradeStatus);
    }

    @Override
    public int hashCode() {
        return Objects.hash(featureUpgradeStatuses, upgradeStatus);
    }

    @Override
    public String toString() {
        return "GetFeatureUpgradeStatusResponse{"
            + "featureUpgradeStatuses="
            + featureUpgradeStatuses
            + ", upgradeStatus='"
            + upgradeStatus
            + '\''
            + '}';
    }

    public enum UpgradeStatus {
        NO_MIGRATION_NEEDED,
        MIGRATION_NEEDED,
        IN_PROGRESS,
        ERROR;

        public static UpgradeStatus combine(UpgradeStatus... statuses) {
            int statusOrd = 0;
            for (UpgradeStatus status : statuses) {
                statusOrd = Math.max(status.ordinal(), statusOrd);
            }
            return UpgradeStatus.values()[statusOrd];
        }
    }

    /**
     * A class for a particular feature, showing whether it needs to be upgraded and the earliest
     * Elasticsearch version used to create one of this feature's system indices.
     */
    public static class FeatureUpgradeStatus implements Writeable, ToXContentObject {
        private final String featureName;
        private final IndexVersion minimumIndexVersion;
        private final UpgradeStatus upgradeStatus;
        private final List<IndexInfo> indexInfos;

        /**
         * @param featureName Name of the feature
         * @param minimumIndexVersion Earliest Elasticsearch version used to create a system index for this feature
         * @param upgradeStatus Whether the feature needs to be upgraded
         * @param indexInfos A list of this feature's concrete indices and the Elasticsearch version that created them
         */
        public FeatureUpgradeStatus(
            String featureName,
            IndexVersion minimumIndexVersion,
            UpgradeStatus upgradeStatus,
            List<IndexInfo> indexInfos
        ) {
            this.featureName = featureName;
            this.minimumIndexVersion = minimumIndexVersion;
            this.upgradeStatus = upgradeStatus;
            this.indexInfos = indexInfos;
        }

        /**
         * @param in A stream input for a serialized feature status object
         * @throws IOException if we can't deserialize the object
         */
        public FeatureUpgradeStatus(StreamInput in) throws IOException {
            this.featureName = in.readString();
            this.minimumIndexVersion = IndexVersion.readVersion(in);
            this.upgradeStatus = in.readEnum(UpgradeStatus.class);
            this.indexInfos = in.readCollectionAsImmutableList(IndexInfo::new);
        }

        public String getFeatureName() {
            return this.featureName;
        }

        public IndexVersion getMinimumIndexVersion() {
            return this.minimumIndexVersion;
        }

        public UpgradeStatus getUpgradeStatus() {
            return this.upgradeStatus;
        }

        public List<IndexInfo> getIndexVersions() {
            return this.indexInfos;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(this.featureName);
            IndexVersion.writeVersion(this.minimumIndexVersion, out);
            out.writeEnum(this.upgradeStatus);
            out.writeCollection(this.indexInfos);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("feature_name", this.featureName);
            builder.field("minimum_index_version", this.minimumIndexVersion.toReleaseVersion());
            builder.field("migration_status", this.upgradeStatus);
            builder.startArray("indices");
            for (IndexInfo version : this.indexInfos) {
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
            FeatureUpgradeStatus that = (FeatureUpgradeStatus) o;
            return Objects.equals(featureName, that.featureName)
                && Objects.equals(minimumIndexVersion, that.minimumIndexVersion)
                && Objects.equals(upgradeStatus, that.upgradeStatus)
                && Objects.equals(indexInfos, that.indexInfos);
        }

        @Override
        public int hashCode() {
            return Objects.hash(featureName, minimumIndexVersion, upgradeStatus, indexInfos);
        }

        @Override
        public String toString() {
            return "FeatureUpgradeStatus{"
                + "featureName='"
                + featureName
                + '\''
                + ", minimumIndexVersion='"
                + minimumIndexVersion
                + '\''
                + ", upgradeStatus='"
                + upgradeStatus
                + '\''
                + ", indexInfos="
                + indexInfos
                + '}';
        }
    }

    /**
     * A data class that holds an index name and the version of Elasticsearch with which that index was created
     */
    public static class IndexInfo implements Writeable, ToXContentObject {
        private static final Map<String, String> STACK_TRACE_ENABLED_PARAMS = Map.of(
            ElasticsearchException.REST_EXCEPTION_SKIP_STACK_TRACE,
            "false"
        );

        private final String indexName;
        private final IndexVersion version;
        @Nullable
        private final Exception exception; // Present if this index failed

        /**
         * @param indexName Name of the index
         * @param version   Index version
         * @param exception The exception that this index's migration failed with, if applicable
         */
        public IndexInfo(String indexName, IndexVersion version, Exception exception) {
            this.indexName = indexName;
            this.version = version;
            this.exception = exception;
        }

        /**
         * @param in A stream input for a serialized index version object
         * @throws IOException if we can't deserialize the object
         */
        public IndexInfo(StreamInput in) throws IOException {
            this.indexName = in.readString();
            this.version = IndexVersion.readVersion(in);
            boolean hasException = in.readBoolean();
            if (hasException) {
                this.exception = in.readException();
            } else {
                this.exception = null;
            }
        }

        public String getIndexName() {
            return this.indexName;
        }

        public IndexVersion getVersion() {
            return this.version;
        }

        public Exception getException() {
            return this.exception;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(this.indexName);
            IndexVersion.writeVersion(this.version, out);
            if (exception != null) {
                out.writeBoolean(true);
                out.writeException(this.exception);
            } else {
                out.writeBoolean(false);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            Params exceptionParams = new DelegatingMapParams(STACK_TRACE_ENABLED_PARAMS, params);

            builder.startObject();
            builder.field("index", this.indexName);
            builder.field("version", this.version.toReleaseVersion());
            if (exception != null) {
                builder.startObject("failure_cause");
                {
                    ElasticsearchException.generateFailureXContent(builder, exceptionParams, exception, true);
                }
                builder.endObject();
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IndexInfo that = (IndexInfo) o;
            return indexName.equals(that.indexName) && version.equals(that.version);
        }

        @Override
        public int hashCode() {
            return Objects.hash(indexName, version);
        }

        @Override
        public String toString() {
            return "IndexInfo{"
                + "indexName='"
                + indexName
                + '\''
                + ", version='"
                + version
                + '\''
                + ", exception='"
                + (exception == null ? "null" : exception.getMessage())
                + "'"
                + '}';
        }
    }
}
