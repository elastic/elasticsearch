/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.upgrades;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.upgrades.FeatureMigrationResults.MIGRATION_ADDED_VERSION;

/**
 * The params used to initialize {@link SystemIndexMigrator} when it's initially kicked off.
 *
 * Currently doesn't do anything. In the future, a name of a feature could be used to indicate that only a specific feature should be
 * migrated.
 */
public class SystemIndexMigrationTaskParams implements PersistentTaskParams {

    public static final String SYSTEM_INDEX_UPGRADE_TASK_NAME = "upgrade-system-indices";
    public static final ObjectParser<SystemIndexMigrationTaskParams, Void> PARSER = new ObjectParser<>(
        SYSTEM_INDEX_UPGRADE_TASK_NAME,
        true,
        SystemIndexMigrationTaskParams::new
    );
    static {
        // PARSER.declareString etc...
    }

    public static SystemIndexMigrationTaskParams fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public SystemIndexMigrationTaskParams() {
        // This space intentionally left blank
    }

    public SystemIndexMigrationTaskParams(StreamInput in) {
        // This space intentionally left blank
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // This space intentionally left blank
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return SYSTEM_INDEX_UPGRADE_TASK_NAME;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return MIGRATION_ADDED_VERSION;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof SystemIndexMigrationTaskParams;
    }

    @Override
    public int hashCode() {
        return 0;
    }
}
