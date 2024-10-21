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
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class SecurityMigrationTaskParams implements PersistentTaskParams {
    public static final String TASK_NAME = "security-migration";

    private final int migrationVersion;

    private final boolean migrationNeeded;

    public static final ConstructingObjectParser<SecurityMigrationTaskParams, Void> PARSER = new ConstructingObjectParser<>(
        TASK_NAME,
        true,
        (arr) -> new SecurityMigrationTaskParams((int) arr[0], arr[1] == null || (boolean) arr[1])
    );

    static {
        PARSER.declareInt(constructorArg(), new ParseField("migration_version"));
        PARSER.declareBoolean(optionalConstructorArg(), new ParseField("migration_needed"));
    }

    public SecurityMigrationTaskParams(int migrationVersion, boolean migrationNeeded) {
        this.migrationVersion = migrationVersion;
        this.migrationNeeded = migrationNeeded;
    }

    public SecurityMigrationTaskParams(StreamInput in) throws IOException {
        this.migrationVersion = in.readInt();
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
            this.migrationNeeded = in.readBoolean();
        } else {
            this.migrationNeeded = true;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(migrationVersion);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
            out.writeBoolean(migrationNeeded);
        }
    }

    @Override
    public String getWriteableName() {
        return TASK_NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_15_0;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field("migration_version", migrationVersion);
        builder.field("migration_needed", migrationNeeded);
        builder.endObject();
        return builder;
    }

    public static SecurityMigrationTaskParams fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public int getMigrationVersion() {
        return migrationVersion;
    }

    public boolean isMigrationNeeded() {
        return migrationNeeded;
    }
}
