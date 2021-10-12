/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.migration;

import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Information about which system features need to be migrated before the next
 * major version.
 */
public class GetFeatureMigrationStatusResponse {

    private static final ParseField FEATURE_MIGRATION_STATUSES = new ParseField("features");
    private static final ParseField MIGRATION_STATUS = new ParseField("migration_status");

    private final List<FeatureMigrationStatus> featureMigrationStatuses;
    private final String migrationStatus;

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<GetFeatureMigrationStatusResponse, Void> PARSER = new ConstructingObjectParser<>(
        "get_feature_migration_response", true, (a, ctx) -> new GetFeatureMigrationStatusResponse(
        (List<FeatureMigrationStatus>) a[0], (String) a[1])
    );

    static {
        PARSER.declareObjectArray(ConstructingObjectParser.optionalConstructorArg(),
            FeatureMigrationStatus::parse, FEATURE_MIGRATION_STATUSES);
        PARSER.declareField(ConstructingObjectParser.constructorArg(),
            (p, c) -> p.text(), MIGRATION_STATUS, ObjectParser.ValueType.STRING);
    }

    /**
     * Constructor for the response object
     * @param featureMigrationStatuses A list of feature, their migration statuses, and other relevant information for upgrading
     * @param migrationStatus Does this feature need to be migrated or not?
     */
    public GetFeatureMigrationStatusResponse(List<FeatureMigrationStatus> featureMigrationStatuses, String migrationStatus) {
        this.featureMigrationStatuses = Objects.nonNull(featureMigrationStatuses) ? featureMigrationStatuses : Collections.emptyList();
        this.migrationStatus = migrationStatus;
    }

    public static GetFeatureMigrationStatusResponse parse(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    public List<FeatureMigrationStatus> getFeatureMigrationStatuses() {
        return featureMigrationStatuses;
    }

    public String getMigrationStatus() {
        return migrationStatus;
    }

    /**
     * This class represents a particular feature and whether it needs to be migrated.
     */
    public static class FeatureMigrationStatus {
        private final String featureName;
        private final String minimumIndexVersion;
        private final String migrationStatus;
        private final List<IndexVersion> indexVersions;

        private static final ParseField FEATURE_NAME = new ParseField("feature_name");
        private static final ParseField MINIMUM_INDEX_VERSION = new ParseField("minimum_index_version");
        private static final ParseField MIGRATION_STATUS = new ParseField("migration_status");
        private static final ParseField INDEX_VERSIONS = new ParseField("indices");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<FeatureMigrationStatus, Void> PARSER = new ConstructingObjectParser<>(
            "feature_migration_status", true, (a, ctx) -> new FeatureMigrationStatus(
            (String) a[0], (String) a[1], (String) a[2], (List<IndexVersion>) a[3]));

        static {
            PARSER.declareField(ConstructingObjectParser.constructorArg(),
                (p, c) -> p.text(), FEATURE_NAME, ObjectParser.ValueType.STRING);
            PARSER.declareField(ConstructingObjectParser.constructorArg(),
                (p, c) -> p.text(), MINIMUM_INDEX_VERSION, ObjectParser.ValueType.STRING);
            PARSER.declareField(ConstructingObjectParser.constructorArg(),
                (p, c) -> p.text(), MIGRATION_STATUS, ObjectParser.ValueType.STRING);
            PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), IndexVersion::parse, INDEX_VERSIONS);
        }

        /**
         * A feature migration status object
         * @param featureName Name of the feature
         * @param minimumIndexVersion The earliest version of Elasticsearch used to create one of this feature's system indices
         * @param migrationStatus Whether this feature needs to be migrated
         * @param indexVersions A list of individual indices and which version of Elasticsearch created them
         */
        public FeatureMigrationStatus(
            String featureName,
            String minimumIndexVersion,
            String migrationStatus,
            List<IndexVersion> indexVersions
        ) {
            this.featureName = featureName;
            this.minimumIndexVersion = minimumIndexVersion;
            this.migrationStatus = migrationStatus;
            this.indexVersions = indexVersions;
        }

        public static FeatureMigrationStatus parse(XContentParser parser, Void ctx) {
            return PARSER.apply(parser, null);
        }

        public String getFeatureName() {
            return featureName;
        }

        public String getMinimumIndexVersion() {
            return minimumIndexVersion;
        }

        public String getMigrationStatus() {
            return migrationStatus;
        }

        public List<IndexVersion> getIndexVersions() {
            return indexVersions;
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
    }

    /**
     * A class representing an index and the version of Elasticsearch that created it.
     */
    public static class IndexVersion {
        private final String indexName;
        private final String version;

        /**
         * Constructor
         * @param indexName Name of a concrete index
         * @param version Version of Elasticsearch used to create the index
         */
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
