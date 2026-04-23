/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

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
import java.util.Collections;
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
    private final Map<String, DataSourceSetting> settings;

    public DataSource(String name, String type, @Nullable String description, Map<String, DataSourceSetting> settings) {
        this.name = Objects.requireNonNull(name, "name must not be null");
        this.type = Objects.requireNonNull(type, "type must not be null");
        this.description = description;
        this.settings = Collections.unmodifiableMap(Objects.requireNonNull(settings, "settings must not be null"));
    }

    public DataSource(StreamInput in) throws IOException {
        this.name = in.readString();
        this.type = in.readString();
        this.description = in.readOptionalString();
        // readMap returns a mutable HashMap when non-empty; wrap to preserve the class invariant that settings is unmodifiable
        this.settings = Collections.unmodifiableMap(in.readMap(DataSourceSetting::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(type);
        out.writeOptionalString(description);
        out.writeMap(settings, StreamOutput::writeWriteable);
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

    public Map<String, DataSourceSetting> settings() {
        return settings;
    }

    /**
     * Flatten settings for the query pipeline. Values are plaintext including secrets. Each setting is accessed
     * through its classification-specific accessor (non-secret vs secret), so this iteration is explicit about
     * producing plaintext for both kinds.
     */
    public Map<String, Object> toUnencryptedMap() {
        Map<String, Object> result = new HashMap<>();
        for (var entry : settings.entrySet()) {
            DataSourceSetting setting = entry.getValue();
            Object plaintext;
            if (setting.secret()) {
                try (var secure = setting.secretValue()) {
                    plaintext = secure == null ? null : secure.toString();
                }
            } else {
                plaintext = setting.nonSecretValue();
            }
            result.put(entry.getKey(), plaintext);
        }
        return Collections.unmodifiableMap(result);
    }

    /** Settings with secrets masked. Safe for REST responses. */
    public Map<String, Object> toPresentationMap() {
        Map<String, Object> result = new HashMap<>();
        for (var entry : settings.entrySet()) {
            result.put(entry.getKey(), entry.getValue().presentationValue());
        }
        return Collections.unmodifiableMap(result);
    }

    public static DataSource fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    /**
     * Emits the in-memory plaintext representation, including secret values as-is. Used for cluster-state
     * persistence (GATEWAY context only) and is not reached from the API or SNAPSHOT contexts because
     * {@link DataSourceMetadata#context()} excludes both. Callers producing REST responses should route through
     * {@link #toPresentationMap()}. See {@link DataSourceSetting} for the encryption-boundary contract.
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
        for (var entry : settings.entrySet()) {
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
        // Uses toPresentationMap() so secret values appear as "::es_redacted::" rather than their raw form.
        return "DataSource{name='"
            + name
            + "', type='"
            + type
            + "', description='"
            + description
            + "', settings="
            + toPresentationMap()
            + "}";
    }
}
