/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.upgrades;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.upgrades.SystemIndexMigrationTaskParams.SYSTEM_INDEX_UPGRADE_TASK_NAME;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Contains the current state of system index migration progress. Used to resume runs if there's a node failure during migration.
 */
public class SystemIndexMigrationTaskState implements PersistentTaskState {
    private static final ParseField CURRENT_INDEX_FIELD = new ParseField("current_index");
    private static final ParseField CURRENT_FEATURE_FIELD = new ParseField("current_feature");
    // scope for testing
    static final ParseField FEATURE_METADATA_MAP_FIELD = new ParseField("feature_metadata");

    @SuppressWarnings(value = "unchecked")
    static final ConstructingObjectParser<SystemIndexMigrationTaskState, Void> PARSER = new ConstructingObjectParser<>(
        SYSTEM_INDEX_UPGRADE_TASK_NAME,
        true,
        args -> new SystemIndexMigrationTaskState((String) args[0], (String) args[1], (Map<String, Object>) args[2])
    );

    static {
        PARSER.declareString(constructorArg(), CURRENT_INDEX_FIELD);
        PARSER.declareString(constructorArg(), CURRENT_FEATURE_FIELD);
        PARSER.declareObject(constructorArg(), (p, c) -> p.map(), FEATURE_METADATA_MAP_FIELD);
    }

    private final String currentIndex;
    private final String currentFeature;
    private final Map<String, Object> featureCallbackMetadata;

    public SystemIndexMigrationTaskState(String currentIndex, String currentFeature, Map<String, Object> featureCallbackMetadata) {
        this.currentIndex = Objects.requireNonNull(currentIndex);
        this.currentFeature = Objects.requireNonNull(currentFeature);
        this.featureCallbackMetadata = featureCallbackMetadata == null ? new HashMap<>() : featureCallbackMetadata;
    }

    public SystemIndexMigrationTaskState(StreamInput in) throws IOException {
        this.currentIndex = in.readString();
        this.currentFeature = in.readString();
        this.featureCallbackMetadata = in.readMap();
    }

    /**
     * Gets the name of the index that's currently being migrated.
     */
    public String getCurrentIndex() {
        return currentIndex;
    }

    /**
     * Gets the name of the feature which owns the index that's currently being migrated.
     */
    public String getCurrentFeature() {
        return currentFeature;
    }

    /**
     * Retrieves metadata stored by the pre-upgrade hook, intended for consumption by the post-migration hook.
     * See {@link org.elasticsearch.plugins.SystemIndexPlugin#prepareForIndicesMigration(ClusterService, Client, ActionListener)} and
     * {@link org.elasticsearch.plugins.SystemIndexPlugin#indicesMigrationComplete(Map, ClusterService, Client, ActionListener)} for
     * details on the pre- and post-migration hooks.
     */
    public Map<String, Object> getFeatureCallbackMetadata() {
        return featureCallbackMetadata;
    }

    public static SystemIndexMigrationTaskState fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(CURRENT_INDEX_FIELD.getPreferredName(), currentIndex);
            builder.field(CURRENT_FEATURE_FIELD.getPreferredName(), currentFeature);
            builder.field(FEATURE_METADATA_MAP_FIELD.getPreferredName(), featureCallbackMetadata);
        }
        builder.endObject();
        return null;
    }

    @Override
    public String getWriteableName() {
        return SYSTEM_INDEX_UPGRADE_TASK_NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(currentIndex);
        out.writeString(currentFeature);
        out.writeMap(featureCallbackMetadata);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o instanceof SystemIndexMigrationTaskState) == false) return false;
        SystemIndexMigrationTaskState that = (SystemIndexMigrationTaskState) o;
        return currentIndex.equals(that.currentIndex)
            && currentFeature.equals(that.currentFeature)
            && featureCallbackMetadata.equals(that.featureCallbackMetadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(currentIndex, currentFeature, featureCallbackMetadata);
    }

    @Override
    public String toString() {
        return "SystemIndexMigrationTaskState{"
            + "currentIndex='"
            + currentIndex
            + '\''
            + ", currentFeature='"
            + currentFeature
            + '\''
            + ", featureCallbackMetadata="
            + featureCallbackMetadata
            + '}';
    }
}
