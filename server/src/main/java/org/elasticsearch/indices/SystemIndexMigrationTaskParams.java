/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class SystemIndexMigrationTaskParams implements PersistentTaskParams {
    public static final String TASK_NAME = "system-index-migration";

    private final int[] migrationVersions;
    private final String indexName;

    public static final ConstructingObjectParser<SystemIndexMigrationTaskParams, Void> PARSER = new ConstructingObjectParser<>(
        TASK_NAME,
        true,
        (arr) -> new SystemIndexMigrationTaskParams((String) arr[0], (int[]) arr[1])
    );

    static {
        PARSER.declareString(constructorArg(), new ParseField("index_name"));
        PARSER.declareIntArray(constructorArg(), new ParseField("migration_versions"));
    }

    public SystemIndexMigrationTaskParams(String indexName, int[] migrationVersions) {
        this.indexName = indexName;
        this.migrationVersions = migrationVersions;
    }

    public SystemIndexMigrationTaskParams(StreamInput in) throws IOException {
        this.indexName = in.readString();
        this.migrationVersions = in.readIntArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(indexName);
        out.writeIntArray(migrationVersions);
    }

    @Override
    public String getWriteableName() {
        return TASK_NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ADD_SYSTEM_INDEX_MIGRATIONS;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("migration_versions", migrationVersions);
        builder.field("index_name", indexName);
        builder.endObject();
        return builder;
    }

    public static SystemIndexMigrationTaskParams fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public String getIndexName() {
        return indexName;
    }

    public int[] getMigrationVersions() {
        return migrationVersions;
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexName, Arrays.hashCode(migrationVersions));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        SystemIndexMigrationTaskParams other = (SystemIndexMigrationTaskParams) obj;
        return Arrays.equals(this.migrationVersions, other.migrationVersions) && Objects.equals(indexName, other.indexName);
    }

}
