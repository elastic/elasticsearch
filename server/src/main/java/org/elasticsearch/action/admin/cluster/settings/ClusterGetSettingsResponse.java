/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.settings;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * This response is specific to the REST client. {@link org.elasticsearch.action.admin.cluster.state.ClusterStateResponse}
 * is used on the transport layer.
 */
public class ClusterGetSettingsResponse extends ActionResponse implements ToXContentObject {

    private Settings persistentSettings = Settings.EMPTY;
    private Settings transientSettings = Settings.EMPTY;
    private Settings defaultSettings = Settings.EMPTY;

    static final String PERSISTENT_FIELD = "persistent";
    static final String TRANSIENT_FIELD = "transient";
    static final String DEFAULTS_FIELD = "defaults";

    private static final ConstructingObjectParser<ClusterGetSettingsResponse, Void> PARSER = new ConstructingObjectParser<>(
        "cluster_get_settings_response",
        true,
        a -> {
            Settings defaultSettings = a[2] == null ? Settings.EMPTY : (Settings) a[2];
            return new ClusterGetSettingsResponse((Settings) a[0], (Settings) a[1], defaultSettings);
        }
    );
    static {
        PARSER.declareObject(constructorArg(), (p, c) -> Settings.fromXContent(p), new ParseField(PERSISTENT_FIELD));
        PARSER.declareObject(constructorArg(), (p, c) -> Settings.fromXContent(p), new ParseField(TRANSIENT_FIELD));
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> Settings.fromXContent(p), new ParseField(DEFAULTS_FIELD));
    }

    public ClusterGetSettingsResponse(Settings persistentSettings, Settings transientSettings, Settings defaultSettings) {
        if (persistentSettings != null) {
            this.persistentSettings = persistentSettings;
        }
        if (transientSettings != null) {
            this.transientSettings = transientSettings;
        }
        if (defaultSettings != null) {
            this.defaultSettings = defaultSettings;
        }
    }

    /**
     * Returns the persistent settings for the cluster
     * @return Settings
     */
    public Settings getPersistentSettings() {
        return persistentSettings;
    }

    /**
     * Returns the transient settings for the cluster
     * @return Settings
     */
    public Settings getTransientSettings() {
        return transientSettings;
    }

    /**
     * Returns the default settings for the cluster (only if {@code include_defaults} was set to true in the request)
     * @return Settings
     */
    public Settings getDefaultSettings() {
        return defaultSettings;
    }

    /**
     * Returns the string value of the setting for the specified index. The order of search is first
     * in persistent settings the transient settings and finally the default settings.
     * @param setting the name of the setting to get
     * @return String
     */
    public String getSetting(String setting) {
        if (persistentSettings.hasValue(setting)) {
            return persistentSettings.get(setting);
        } else if (transientSettings.hasValue(setting)) {
            return transientSettings.get(setting);
        } else if (defaultSettings.hasValue(setting)) {
            return defaultSettings.get(setting);
        } else {
            return null;
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.startObject(PERSISTENT_FIELD);
        persistentSettings.toXContent(builder, params);
        builder.endObject();

        builder.startObject(TRANSIENT_FIELD);
        transientSettings.toXContent(builder, params);
        builder.endObject();

        if (defaultSettings.isEmpty() == false) {
            builder.startObject(DEFAULTS_FIELD);
            defaultSettings.toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    public static ClusterGetSettingsResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterGetSettingsResponse that = (ClusterGetSettingsResponse) o;
        return Objects.equals(transientSettings, that.transientSettings)
            && Objects.equals(persistentSettings, that.persistentSettings)
            && Objects.equals(defaultSettings, that.defaultSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transientSettings, persistentSettings, defaultSettings);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {}
}
