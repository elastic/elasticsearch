/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
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
 * Represents a datasource definition stored in cluster state. A datasource holds
 * connection settings (credentials, endpoints, auth) for an external data provider.
 */
public final class Datasource implements Writeable, ToXContentObject {

    private static final ParseField NAME = new ParseField("name");
    private static final ParseField TYPE_FIELD = new ParseField("type");
    private static final ParseField DESCRIPTION = new ParseField("description");
    private static final ParseField SETTINGS = new ParseField("settings");

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<Datasource, Void> PARSER = new ConstructingObjectParser<>(
        "datasource",
        false,
        (args, ctx) -> new Datasource((String) args[0], (String) args[1], (String) args[2], (Map<String, DataSourceStoredSetting>) args[3])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), TYPE_FIELD);
        PARSER.declareStringOrNull(ConstructingObjectParser.optionalConstructorArg(), DESCRIPTION);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> {
            Map<String, DataSourceStoredSetting> settings = new HashMap<>();
            while (p.nextToken() != XContentParser.Token.END_OBJECT) {
                String settingName = p.currentName();
                settings.put(settingName, DataSourceStoredSetting.fromXContent(p));
            }
            return settings;
        }, SETTINGS);
    }

    private final String name;
    private final String type;
    private final String description;
    private final Map<String, DataSourceStoredSetting> settings;

    public Datasource(String name, String type, String description, Map<String, DataSourceStoredSetting> settings) {
        this.name = Objects.requireNonNull(name, "datasource name must not be null");
        this.type = Objects.requireNonNull(type, "datasource type must not be null");
        this.description = description;
        this.settings = Collections.unmodifiableMap(Objects.requireNonNull(settings, "datasource settings must not be null"));
    }

    public Datasource(StreamInput in) throws IOException {
        this.name = in.readString();
        this.type = in.readString();
        this.description = in.readOptionalString();
        // readMap returns a mutable HashMap when non-empty; wrap to preserve the class invariant that settings is unmodifiable
        this.settings = Collections.unmodifiableMap(in.readMap(DataSourceStoredSetting::new));
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

    public Map<String, DataSourceStoredSetting> settings() {
        return settings;
    }

    /**
     * Flatten settings for the query pipeline, decrypting values. Scaffolding for the CRUD REST API follow-up;
     * not invoked by anything in this PR. The query-execution path will call this to get the plaintext settings
     * map needed to construct storage/format clients.
     */
    public Map<String, Object> toPlainMap() {
        Map<String, Object> result = new HashMap<>();
        for (var entry : settings.entrySet()) {
            result.put(entry.getKey(), entry.getValue().decryptedValue());
        }
        return result;
    }

    /**
     * Settings with secrets masked for API responses. Scaffolding for the CRUD REST API follow-up; not invoked by
     * anything in this PR. The GET handler will call this so secret fields appear as {@code "**********"} rather
     * than their stored (eventually-encrypted) values.
     */
    public Map<String, Object> toMaskedMap() {
        Map<String, Object> result = new HashMap<>();
        for (var entry : settings.entrySet()) {
            result.put(entry.getKey(), entry.getValue().maskedOrDecryptedValue());
        }
        return result;
    }

    public static Datasource fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

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
        Datasource that = (Datasource) o;
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
        return Strings.toString(this);
    }
}
