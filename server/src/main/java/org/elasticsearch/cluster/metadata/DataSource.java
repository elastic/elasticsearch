/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.TransportVersion;
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
 *
 * <p>The {@code uuid} field is a rename-stable, immutable identifier populated at PUT creation time
 * (via {@code UUIDs.randomBase64UUID()} on the master-side validator). It is reserved for use by
 * per-data-source key derivation when the security infrastructure grows that capability; today it
 * is persisted but not otherwise consumed. Legacy entries that predate the field parse with
 * {@code uuid == null}; the next PUT for such an entry populates it.
 */
public final class DataSource implements Writeable, ToXContentObject {

    /**
     * Transport-version gate for the {@code uuid} field on {@link DataSource}. Pre-version peers
     * neither read nor write the field; reads return {@code null} on the recipient side.
     * Shares the {@code data_source_encryption} name with the encryption wire-format gate since
     * both ship together in the same wire-format evolution.
     */
    public static final TransportVersion DATA_SOURCE_ADD_UUID_FIELD = TransportVersion.fromName("data_source_encryption");

    private static final ParseField NAME = new ParseField("name");
    private static final ParseField TYPE_FIELD = new ParseField("type");
    private static final ParseField DESCRIPTION = new ParseField("description");
    private static final ParseField SETTINGS = new ParseField("settings");
    private static final ParseField UUID = new ParseField("uuid");

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<DataSource, Void> PARSER = new ConstructingObjectParser<>(
        "data_source",
        false,
        (args, ctx) -> new DataSource(
            (String) args[0],
            (String) args[1],
            (String) args[2],
            (Map<String, DataSourceSetting>) args[3],
            (String) args[4]
        )
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
        PARSER.declareStringOrNull(ConstructingObjectParser.optionalConstructorArg(), UUID);
    }

    private final String name;
    private final String type;
    private final String description;
    private final Map<String, DataSourceSetting> settings;
    private final String uuid;

    public DataSource(
        String name,
        String type,
        @Nullable String description,
        Map<String, DataSourceSetting> settings,
        @Nullable String uuid
    ) {
        this.name = Objects.requireNonNull(name, "name must not be null");
        this.type = Objects.requireNonNull(type, "type must not be null");
        this.description = description;
        this.settings = Collections.unmodifiableMap(Objects.requireNonNull(settings, "settings must not be null"));
        this.uuid = uuid;
    }

    public DataSource(StreamInput in) throws IOException {
        this.name = in.readString();
        this.type = in.readString();
        this.description = in.readOptionalString();
        // readMap returns a mutable HashMap when non-empty; wrap to preserve the class invariant that settings is unmodifiable
        this.settings = Collections.unmodifiableMap(in.readMap(DataSourceSetting::new));
        // uuid is gated: pre-version peers don't write it, so reads default to null
        this.uuid = in.getTransportVersion().supports(DATA_SOURCE_ADD_UUID_FIELD) ? in.readOptionalString() : null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(type);
        out.writeOptionalString(description);
        out.writeMap(settings, StreamOutput::writeWriteable);
        if (out.getTransportVersion().supports(DATA_SOURCE_ADD_UUID_FIELD)) {
            out.writeOptionalString(uuid);
        }
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
     * The data source's rename-stable identifier. {@code null} for legacy entries that predate the field;
     * non-null for every entry created or re-PUT-ed after the field shipped.
     */
    @Nullable
    public String uuid() {
        return uuid;
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
        if (uuid != null) {
            builder.field(UUID.getPreferredName(), uuid);
        }
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
            && Objects.equals(settings, that.settings)
            && Objects.equals(uuid, that.uuid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, description, settings, uuid);
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
            + "', uuid='"
            + uuid
            + "', settings="
            + toPresentationMap()
            + "}";
    }
}
