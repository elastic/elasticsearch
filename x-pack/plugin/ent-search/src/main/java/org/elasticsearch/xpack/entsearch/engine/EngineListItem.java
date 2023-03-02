/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch.engine;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * This class is used for returning information for lists of engines, to avoid including all Engine information
 * which can be retrieved using subsequent Get Engine requests
 */
public class EngineListItem implements Writeable, ToXContentObject {

    public static final ParseField NAME_FIELD = new ParseField("name");
    public static final ParseField INDICES_FIELD = new ParseField("indices");
    public static final ParseField ENGINE_ALIAS_NAME_FIELD = new ParseField("engine_alias");
    public static final ParseField ANALYTICS_COLLECTION_NAME_FIELD = new ParseField("analytics_collection_name");
    private final String name;
    private final String[] indices;
    private final String engineAlias;
    private final String analyticsCollectionName;

    public EngineListItem(String name, String[] indices, String engineAlias, @Nullable String analyticsCollectionName) {
        this.name = name;
        this.indices = indices;
        this.engineAlias = engineAlias;
        this.analyticsCollectionName = analyticsCollectionName;
    }

    public EngineListItem(StreamInput in) throws IOException {
        this.name = in.readString();
        this.indices = in.readStringArray();
        this.engineAlias = in.readString();
        this.analyticsCollectionName = in.readOptionalString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NAME_FIELD.getPreferredName(), name);
        builder.field(INDICES_FIELD.getPreferredName(), indices);
        builder.field(ENGINE_ALIAS_NAME_FIELD.getPreferredName(), engineAlias);
        if (analyticsCollectionName != null) {
            builder.field(ANALYTICS_COLLECTION_NAME_FIELD.getPreferredName(), analyticsCollectionName);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeStringArray(indices);
        out.writeString(engineAlias);
        out.writeOptionalString(analyticsCollectionName);
    }

    public String name() {
        return name;
    }

    public String[] indices() {
        return indices;
    }

    public String engineAlias() {
        return engineAlias;
    }

    public String analyticsCollectionName() {
        return analyticsCollectionName;
    }
}
