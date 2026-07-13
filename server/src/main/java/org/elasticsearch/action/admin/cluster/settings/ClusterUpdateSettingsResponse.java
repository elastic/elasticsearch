/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.settings;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * A response for a cluster update settings action.
 */
public class ClusterUpdateSettingsResponse extends AcknowledgedResponse {

    static final ParseField PERSISTENT = new ParseField("persistent");
    static final ParseField TRANSIENT = new ParseField("transient");

    final Settings transientSettings;
    final Settings persistentSettings;

    ClusterUpdateSettingsResponse(StreamInput in) throws IOException {
        super(in);
        transientSettings = Settings.readSettingsFromStream(in);
        persistentSettings = Settings.readSettingsFromStream(in);
    }

    public ClusterUpdateSettingsResponse(boolean acknowledged, Settings transientSettings, Settings persistentSettings) {
        super(acknowledged);
        this.persistentSettings = persistentSettings;
        this.transientSettings = transientSettings;
    }

    public Settings getTransientSettings() {
        return transientSettings;
    }

    public Settings getPersistentSettings() {
        return persistentSettings;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        transientSettings.writeTo(out);
        persistentSettings.writeTo(out);
    }

    @Override
    protected void addCustomFields(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(PERSISTENT.getPreferredName());
        persistentSettings.toXContent(builder, params);
        builder.endObject();
        builder.startObject(TRANSIENT.getPreferredName());
        transientSettings.toXContent(builder, params);
        builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o)) {
            ClusterUpdateSettingsResponse that = (ClusterUpdateSettingsResponse) o;
            return Objects.equals(transientSettings, that.transientSettings) && Objects.equals(persistentSettings, that.persistentSettings);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), transientSettings, persistentSettings);
    }
}
