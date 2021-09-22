/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.migration;

import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class GetFeatureUpgradeStatusResponse {

    private static final ParseField FEATURE_UPGRADE_STATUSES = new ParseField("features");
    private static final ParseField UPGRADE_STATUS = new ParseField("upgrade_status");

    private final List<FeatureUpgradeStatus> featureUpgradeStatuses;
    private final String upgradeStatus;

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<GetFeatureUpgradeStatusResponse, Void> PARSER = new ConstructingObjectParser<>(
        "get_feature_upgrade_response", true, (a, ctx) -> new GetFeatureUpgradeStatusResponse(
        (List<FeatureUpgradeStatus>) a[0], (String) a[1])
    );

    static {
        PARSER.declareObjectArray(ConstructingObjectParser.optionalConstructorArg(),
            FeatureUpgradeStatus::parse, FEATURE_UPGRADE_STATUSES);
        PARSER.declareField(ConstructingObjectParser.constructorArg(),
            (p, c) -> p.text(), UPGRADE_STATUS, ObjectParser.ValueType.STRING);
    }

    public GetFeatureUpgradeStatusResponse(List<FeatureUpgradeStatus> featureUpgradeStatuses, String upgradeStatus) {
        this.featureUpgradeStatuses = Objects.nonNull(featureUpgradeStatuses) ? featureUpgradeStatuses : Collections.emptyList();
        this.upgradeStatus = upgradeStatus;
    }

    public static GetFeatureUpgradeStatusResponse parse(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    public List<FeatureUpgradeStatus> getFeatureUpgradeStatuses() {
        return featureUpgradeStatuses;
    }

    public String getUpgradeStatus() {
        return upgradeStatus;
    }

    public static class FeatureUpgradeStatus {
        private final String featureName;
        private final String minimumIndexVersion;
        private final String upgradeStatus;
        private final List<IndexVersion> indexVersions;

        private static final ParseField FEATURE_NAME = new ParseField("feature_name");
        private static final ParseField MINIMUM_INDEX_VERSION = new ParseField("minimum_index_version");
        private static final ParseField UPGRADE_STATUS = new ParseField("upgrade_status");
        private static final ParseField INDEX_VERSIONS = new ParseField("indices");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<FeatureUpgradeStatus, Void> PARSER = new ConstructingObjectParser<>(
            "feature_upgrade_status", true, (a, ctx) -> new FeatureUpgradeStatus(
            (String) a[0], (String) a[1], (String) a[2], (List<IndexVersion>) a[3]));

        static {
            PARSER.declareField(ConstructingObjectParser.constructorArg(),
                (p, c) -> p.text(), FEATURE_NAME, ObjectParser.ValueType.STRING);
            PARSER.declareField(ConstructingObjectParser.constructorArg(),
                (p, c) -> p.text(), MINIMUM_INDEX_VERSION, ObjectParser.ValueType.STRING);
            PARSER.declareField(ConstructingObjectParser.constructorArg(),
                (p, c) -> p.text(), UPGRADE_STATUS, ObjectParser.ValueType.STRING);
            PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), IndexVersion::parse, INDEX_VERSIONS);
        }

        public FeatureUpgradeStatus(
            String featureName,
            String minimumIndexVersion,
            String upgradeStatus,
            List<IndexVersion> indexVersions
        ) {
            this.featureName = featureName;
            this.minimumIndexVersion = minimumIndexVersion;
            this.upgradeStatus = upgradeStatus;
            this.indexVersions = indexVersions;
        }

        public static FeatureUpgradeStatus parse(XContentParser parser, Void ctx) {
            return PARSER.apply(parser, null);
        }

        public String getFeatureName() {
            return featureName;
        }

        public String getMinimumIndexVersion() {
            return minimumIndexVersion;
        }

        public String getUpgradeStatus() {
            return upgradeStatus;
        }

        public List<IndexVersion> getIndexVersions() {
            return indexVersions;
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

    public static class IndexVersion {
        private final String indexName;
        private final String version;

        public IndexVersion(String indexName, String version) {
            this.indexName = indexName;
            this.version = version;
        }

        private static final ParseField INDEX_NAME = new ParseField("index");
        private static final ParseField VERSION = new ParseField("version");

        private static final ConstructingObjectParser<IndexVersion, Void> PARSER = new ConstructingObjectParser<>(
            "index_version", true, (a, ctx) -> new IndexVersion((String) a[0], (String) a[1])
        );

        static {
            PARSER.declareField(ConstructingObjectParser.constructorArg(),
                (p, c) -> p.text(), INDEX_NAME, ObjectParser.ValueType.STRING);
            PARSER.declareField(ConstructingObjectParser.constructorArg(),
                (p, c) -> p.text(), VERSION, ObjectParser.ValueType.STRING);
        }

        public static IndexVersion parse(XContentParser parser, Void ctx) {
            return PARSER.apply(parser, ctx);
        }

        public String getIndexName() {
            return indexName;
        }

        public String getVersion() {
            return version;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IndexVersion that = (IndexVersion) o;
            return Objects.equals(indexName, that.indexName) && Objects.equals(version, that.version);
        }

        @Override
        public int hashCode() {
            return Objects.hash(indexName, version);
        }
    }
}
