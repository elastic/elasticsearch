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

public class GetFeatureUpgradeStatusResponse extends ActionResponse implements ToXContentObject {

    private final List<FeatureUpgradeStatus> featureUpgradeStatuses;
    private final String upgradeStatus;

    public GetFeatureUpgradeStatusResponse(List<FeatureUpgradeStatus> statuses, String upgradeStatus) {
        this.featureUpgradeStatuses = Objects.nonNull(statuses) ? statuses : Collections.emptyList();
        this.upgradeStatus = upgradeStatus;
    }

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

    public static class FeatureUpgradeStatus implements Writeable, ToXContentObject {
        private final String featureName;
        private final String minimumIndexVersion;
        private final String upgradeStatus;
        private final List<IndexVersion> indexVersions;

        public FeatureUpgradeStatus(String featureName, String minimumIndexVersion,
                                    String upgradeStatus, List<IndexVersion> indexVersions) {
            this.featureName = featureName;
            this.minimumIndexVersion = minimumIndexVersion;
            this.upgradeStatus = upgradeStatus;
            this.indexVersions = indexVersions;
        }

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

    public static class IndexVersion implements Writeable, ToXContentObject {
        private final String indexName;
        private final String version;

        public IndexVersion(String indexName, String version) {
            this.indexName = indexName;
            this.version = version;
        }

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
