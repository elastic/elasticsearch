/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.metadata;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a data source definition stored in cluster state. A data source holds
 * connection settings (credentials, endpoints, auth) for an external data provider.
 */
public final class DataSource implements Writeable, ToXContentObject {

    private static final ParseField NAME = new ParseField("name");
    private static final ParseField TYPE_FIELD = new ParseField("type");
    private static final ParseField DESCRIPTION = new ParseField("description");
    private static final ParseField SETTINGS = new ParseField("settings");

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<DataSource, Void> PARSER = new ConstructingObjectParser<>(
        "data_source",
        false,
        (args, ctx) -> new DataSource((String) args[0], (String) args[1], (String) args[2], (Map<String, DataSourceSetting>) args[3])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), TYPE_FIELD);
        PARSER.declareStringOrNull(ConstructingObjectParser.optionalConstructorArg(), DESCRIPTION);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> {
            Map<String, DataSourceSetting> settings = new HashMap<>();
            while (p.nextToken() != XContentParser.Token.END_OBJECT) {
                String settingName = p.currentName();
                settings.put(settingName, DataSourceSetting.fromXContent(p));
            }
            return settings;
        }, SETTINGS);
    }

    private final String name;
    private final String type;
    private final String description;
    private final DataSourceSettings settings;

    public DataSource(String name, String type, @Nullable String description, Map<String, DataSourceSetting> settings) {
        this(name, type, description, new DataSourceSettings(Objects.requireNonNull(settings, "settings must not be null")));
    }

    public DataSource(String name, String type, @Nullable String description, DataSourceSettings settings) {
        this.name = Objects.requireNonNull(name, "name must not be null");
        this.type = Objects.requireNonNull(type, "type must not be null");
        this.description = description;
        this.settings = Objects.requireNonNull(settings, "settings must not be null");
    }

    public DataSource(StreamInput in) throws IOException {
        this.name = in.readString();
        this.type = in.readString();
        this.description = in.readOptionalString();
        this.settings = new DataSourceSettings(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(type);
        out.writeOptionalString(description);
        settings.writeTo(out);
    }

    public String name() {
        return name;
    }

    public String type() {
        return type;
    }

    public String description() {
        return description;
    }

    public DataSourceSettings settings() {
        return settings;
    }

    public static DataSource fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    /**
     * Emits the in-memory plaintext representation, including secret values as-is. Used for cluster-state
     * persistence (GATEWAY context only) and is not reached from the API or SNAPSHOT contexts because
     * {@link DataSourceMetadata#context()} excludes both. Callers producing REST responses should route
     * through {@link DataSourceSettings#toPresentationMap()}. See {@link DataSourceSetting} for the
     * encryption-boundary contract.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NAME.getPreferredName(), name);
        builder.field(TYPE_FIELD.getPreferredName(), type);
        if (description != null) {
            builder.field(DESCRIPTION.getPreferredName(), description);
        }
        builder.startObject(SETTINGS.getPreferredName());
        for (var entry : settings) {
            builder.field(entry.getKey());
            entry.getValue().toXContent(builder, params);
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataSource that = (DataSource) o;
        return Objects.equals(name, that.name)
            && Objects.equals(type, that.type)
            && Objects.equals(description, that.description)
            && Objects.equals(settings, that.settings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, description, settings);
    }

    @Override
    public String toString() {
        // Uses settings.toPresentationMap() so secret values appear as "::es_redacted::" rather than their raw form.
        return "DataSource{name='"
            + name
            + "', type='"
            + type
            + "', description='"
            + description
            + "', settings="
            + settings.toPresentationMap()
            + "}";
    }
}
