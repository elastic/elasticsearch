/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.metadata;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.Metadata;
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

    /** Gates the {@link #enabled} flag on the wire. */
    public static final TransportVersion ESQL_DATASOURCE_STATE = TransportVersion.fromName("esql_datasource_state");

    private static final ParseField NAME = new ParseField("name");
    private static final ParseField TYPE_FIELD = new ParseField("type");
    private static final ParseField DESCRIPTION = new ParseField("description");
    private static final ParseField SETTINGS = new ParseField("settings");
    private static final ParseField STATE = new ParseField("state");
    private static final String STATE_OPEN = "open";
    private static final String STATE_CLOSED = "closed";

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<DataSource, Void> PARSER = new ConstructingObjectParser<>(
        "data_source",
        false,
        (args, ctx) -> new DataSource(
            (String) args[0],
            (String) args[1],
            (String) args[2],
            (Map<String, DataSourceSetting>) args[3],
            isOpen((String) args[4])
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
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), STATE);
    }

    private final String name;
    private final String type;
    private final String description;
    private final DataSourceSettings settings;
    private final boolean enabled;

    public DataSource(String name, String type, @Nullable String description, Map<String, DataSourceSetting> settings) {
        this(name, type, description, new DataSourceSettings(Objects.requireNonNull(settings, "settings must not be null")), true);
    }

    public DataSource(String name, String type, @Nullable String description, DataSourceSettings settings) {
        this(name, type, description, settings, true);
    }

    public DataSource(String name, String type, @Nullable String description, Map<String, DataSourceSetting> settings, boolean enabled) {
        this(name, type, description, new DataSourceSettings(Objects.requireNonNull(settings, "settings must not be null")), enabled);
    }

    public DataSource(String name, String type, @Nullable String description, DataSourceSettings settings, boolean enabled) {
        this.name = Objects.requireNonNull(name, "name must not be null");
        this.type = Objects.requireNonNull(type, "type must not be null");
        this.description = description;
        this.settings = Objects.requireNonNull(settings, "settings must not be null");
        this.enabled = enabled;
    }

    public DataSource(StreamInput in) throws IOException {
        this.name = in.readString();
        this.type = in.readString();
        this.description = in.readOptionalString();
        this.settings = new DataSourceSettings(in);
        this.enabled = in.getTransportVersion().supports(ESQL_DATASOURCE_STATE) ? in.readBoolean() : true;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(type);
        out.writeOptionalString(description);
        settings.writeTo(out);
        if (out.getTransportVersion().supports(ESQL_DATASOURCE_STATE)) {
            out.writeBoolean(enabled);
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

    public DataSourceSettings settings() {
        return settings;
    }

    /**
     * Whether this data source is open for query use, the default. When {@code false} it is closed: queries that name a
     * dataset on it fail, wildcards skip those datasets, and new datasets cannot be created on it. Admin GET/PUT of the
     * definition still succeed. The flag is kept as a boolean (and is {@code enabled} on the wire), and the API shows it
     * as {@link #state()}.
     */
    public boolean enabled() {
        return enabled;
    }

    /** {@code "open"} or {@code "closed"}, as the API shows the state, the way an index has an open or closed state. */
    public String state() {
        return enabled ? STATE_OPEN : STATE_CLOSED;
    }

    private static boolean isOpen(String state) {
        if (state == null || STATE_OPEN.equals(state)) {
            return true;
        }
        if (STATE_CLOSED.equals(state)) {
            return false;
        }
        throw new IllegalArgumentException("unknown data source state [" + state + "], expected [open] or [closed]");
    }

    /** Returns a copy of this data source with the given enabled flag. */
    public DataSource withEnabled(boolean enabled) {
        if (this.enabled == enabled) {
            return this;
        }
        return new DataSource(name, type, description, settings, enabled);
    }

    public static DataSource fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        boolean isSnapshot = params != null && Metadata.CONTEXT_MODE_SNAPSHOT.equals(params.param(Metadata.CONTEXT_MODE_PARAM));
        builder.startObject();
        builder.field(NAME.getPreferredName(), name);
        builder.field(TYPE_FIELD.getPreferredName(), type);
        if (description != null) {
            builder.field(DESCRIPTION.getPreferredName(), description);
        }
        builder.startObject(SETTINGS.getPreferredName());
        for (var entry : settings) {
            if (isSnapshot == false || entry.getValue().secret() == false) {
                builder.field(entry.getKey());
                entry.getValue().toXContent(builder, params);
            }
        }
        builder.endObject();
        builder.field(STATE.getPreferredName(), state());
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataSource that = (DataSource) o;
        return enabled == that.enabled
            && Objects.equals(name, that.name)
            && Objects.equals(type, that.type)
            && Objects.equals(description, that.description)
            && Objects.equals(settings, that.settings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, description, settings, enabled);
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
            + ", enabled="
            + enabled
            + "}";
    }
}
