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

public class MigrateSecurityIndexFieldTaskParams implements PersistentTaskParams {
    public static final String TASK_NAME = "migrate-security-index-field";
    private final Boolean migrationNeeded;

    public static final ConstructingObjectParser<MigrateSecurityIndexFieldTaskParams, Void> PARSER = new ConstructingObjectParser<>(
        TASK_NAME,
        true,
        a -> new MigrateSecurityIndexFieldTaskParams((Boolean) a[0])
    );

    static {
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), new ParseField("migrationNeeded"));
    }

    public MigrateSecurityIndexFieldTaskParams(Boolean migrationNeeded) {
        this.migrationNeeded = migrationNeeded;
    }

    public MigrateSecurityIndexFieldTaskParams(StreamInput in) throws IOException {
        this.migrationNeeded = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(migrationNeeded);
    }

    @Override
    public String getWriteableName() {
        return TASK_NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_12_0;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field("migrationNeeded", this.migrationNeeded);
        builder.endObject();
        return builder;
    }

    public static MigrateSecurityIndexFieldTaskParams fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public Boolean getMigrationNeeded() {
        return migrationNeeded;
    }
}
