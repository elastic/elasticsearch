/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.migration;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A response showing whether system features need to be upgraded and, feature by feature, which
 * indices need to be upgraded.
 */
public class GetFeatureUpgradeStatusResponse extends ActionResponse implements ToXContentObject {

    private final List<FeatureUpgradeStatus> featureUpgradeStatuses;
    private final String upgradeStatus;

    /**
     * @param statuses A list of feature statuses
     * @param upgradeStatus Whether system features need to be upgraded
     */
    public GetFeatureUpgradeStatusResponse(List<FeatureUpgradeStatus> statuses, String upgradeStatus) {
        this.featureUpgradeStatuses = Objects.nonNull(statuses) ? statuses : Collections.emptyList();
        this.upgradeStatus = upgradeStatus;
    }

    /**
     * @param in A stream input for a serialized response object
     * @throws IOException if we can't deserialize the object
     */
    public GetFeatureUpgradeStatusResponse(StreamInput in) throws IOException {
        super(in);
        this.featureUpgradeStatuses = in.readList(FeatureUpgradeStatus::new);
        this.upgradeStatus = in.readString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray("features");
        for (FeatureUpgradeStatus featureUpgradeStatus : featureUpgradeStatuses) {
            builder.value(featureUpgradeStatus);
        }
        builder.endArray();
        builder.field("upgrade_status", upgradeStatus);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(this.featureUpgradeStatuses);
        out.writeString(upgradeStatus);
    }

    public List<FeatureUpgradeStatus> getFeatureUpgradeStatuses() {
        return featureUpgradeStatuses;
    }

    public String getUpgradeStatus() {
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

    /**
     * A class for a particular feature, showing whether it needs to be upgraded and the earliest
     * Elasticsearch version used to create one of this feature's system indices.
     */
    public static class FeatureUpgradeStatus implements Writeable, ToXContentObject {
        private final String featureName;
        private final String minimumIndexVersion;
        private final String upgradeStatus;
        private final List<IndexVersion> indexVersions;

        /**
         * @param featureName Name of the feature
         * @param minimumIndexVersion Earliest Elasticsearch version used to create a system index for this feature
         * @param upgradeStatus Whether the feature needs to be upgraded
         * @param indexVersions A list of this feature's concrete indices and the Elasticsearch version that created them
         */
        public FeatureUpgradeStatus(String featureName, String minimumIndexVersion,
                                    String upgradeStatus, List<IndexVersion> indexVersions) {
            this.featureName = featureName;
            this.minimumIndexVersion = minimumIndexVersion;
            this.upgradeStatus = upgradeStatus;
            this.indexVersions = indexVersions;
        }

        /**
         * @param in A stream input for a serialized feature status object
         * @throws IOException if we can't deserialize the object
         */
        public FeatureUpgradeStatus(StreamInput in) throws IOException {
            this.featureName = in.readString();
            this.minimumIndexVersion = in.readString();
            this.upgradeStatus = in.readString();
            this.indexVersions = in.readList(IndexVersion::new);
        }

        public String getFeatureName() {
            return this.featureName;
        }

        public String getMinimumIndexVersion() {
            return this.minimumIndexVersion;
        }

        public String getUpgradeStatus() {
            return this.upgradeStatus;
        }

        public List<IndexVersion> getIndexVersions() {
            return this.indexVersions;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(this.featureName);
            out.writeString(this.minimumIndexVersion);
            out.writeString(this.upgradeStatus);
            out.writeList(this.indexVersions);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("feature_name", this.featureName);
            builder.field("minimum_index_version", this.minimumIndexVersion);
            builder.field("upgrade_status", this.upgradeStatus);
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
            FeatureUpgradeStatus that = (FeatureUpgradeStatus) o;
            return Objects.equals(featureName, that.featureName)
                && Objects.equals(minimumIndexVersion, that.minimumIndexVersion)
                && Objects.equals(upgradeStatus, that.upgradeStatus)
                && Objects.equals(indexVersions, that.indexVersions);
        }

        @Override
        public int hashCode() {
            return Objects.hash(featureName, minimumIndexVersion, upgradeStatus, indexVersions);
        }
    }

    /**
     * A data class that holds an index name and the version of Elasticsearch with which that index was created
     */
    public static class IndexVersion implements Writeable, ToXContentObject {
        private final String indexName;
        private final String version;

        /**
         * @param indexName Name of the index
         * @param version Version of Elasticsearch that created the index
         */
        public IndexVersion(String indexName, String version) {
            this.indexName = indexName;
            this.version = version;
        }

        /**
         * @param in A stream input for a serialized index version object
         * @throws IOException if we can't deserialize the object
         */
        public IndexVersion(StreamInput in) throws IOException {
            this.indexName = in.readString();
            this.version = in.readString();
        }

        public String getIndexName() {
            return this.indexName;
        }

        public String getVersion() {
            return this.version;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(this.indexName);
            out.writeString(this.version);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("index", this.indexName);
            builder.field("version", this.version);
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
    }
}
