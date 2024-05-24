/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.support;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class SecurityMigrationTaskParams implements PersistentTaskParams {
    public static final String TASK_NAME = "security-migration";

    private final int migrationVersion;

    public static final ConstructingObjectParser<SecurityMigrationTaskParams, Void> PARSER = new ConstructingObjectParser<>(
        TASK_NAME,
        true,
        (arr) -> new SecurityMigrationTaskParams((int) arr[0])
    );

    static {
        PARSER.declareInt(constructorArg(), new ParseField("migration_version"));
    }

    public SecurityMigrationTaskParams(int migrationVersion) {
        this.migrationVersion = migrationVersion;
    }

    public SecurityMigrationTaskParams(StreamInput in) throws IOException {
        this.migrationVersion = in.readInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(migrationVersion);
    }

    @Override
    public String getWriteableName() {
        return TASK_NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ADD_METADATA_FLATTENED_TO_ROLES;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field("migration_version", migrationVersion);
        builder.endObject();
        return builder;
    }

    public static SecurityMigrationTaskParams fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public int getMigrationVersion() {
        return migrationVersion;
    }
}
