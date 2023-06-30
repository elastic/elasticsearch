/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.settings;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.settings.Settings.readSettingsFromStream;

/**
 * Request for an update cluster settings action
 */
public class ClusterUpdateSettingsRequest extends AcknowledgedRequest<ClusterUpdateSettingsRequest> implements ToXContentObject {

    private static final ParseField PERSISTENT = new ParseField("persistent");
    private static final ParseField TRANSIENT = new ParseField("transient");

    private static final ObjectParser<ClusterUpdateSettingsRequest, Void> PARSER = new ObjectParser<>(
        "cluster_update_settings_request",
        false,
        ClusterUpdateSettingsRequest::new
    );

    static {
        PARSER.declareObject((r, p) -> r.persistentSettings = p, (p, c) -> Settings.fromXContent(p), PERSISTENT);
        PARSER.declareObject((r, t) -> r.transientSettings = t, (p, c) -> Settings.fromXContent(p), TRANSIENT);
    }

    private Settings transientSettings = Settings.EMPTY;
    private Settings persistentSettings = Settings.EMPTY;

    public ClusterUpdateSettingsRequest(StreamInput in) throws IOException {
        super(in);
        transientSettings = readSettingsFromStream(in);
        persistentSettings = readSettingsFromStream(in);
    }

    public ClusterUpdateSettingsRequest() {}

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (transientSettings.isEmpty() && persistentSettings.isEmpty()) {
            validationException = addValidationError("no settings to update", validationException);
        }
        return validationException;
    }

    /**
     * @deprecated Transient settings are in the process of being removed. Use
     * persistent settings to update your cluster settings instead.
     */
    @Deprecated
    public Settings transientSettings() {
        return transientSettings;
    }

    public Settings persistentSettings() {
        return persistentSettings;
    }

    /**
     * Sets the transient settings to be updated. They will not survive a full cluster restart
     *
     * @deprecated Transient settings are in the process of being removed. Use
     * persistent settings to update your cluster settings instead.
     */
    @Deprecated
    public ClusterUpdateSettingsRequest transientSettings(Settings settings) {
        this.transientSettings = settings;
        return this;
    }

    /**
     * Sets the transient settings to be updated. They will not survive a full cluster restart
     *
     * @deprecated Transient settings are in the process of being removed. Use
     * persistent settings to update your cluster settings instead.
     */
    @Deprecated
    public ClusterUpdateSettingsRequest transientSettings(Settings.Builder settings) {
        this.transientSettings = settings.build();
        return this;
    }

    /**
     * Sets the source containing the transient settings to be updated. They will not survive a full cluster restart
     *
     * @deprecated Transient settings are in the process of being removed. Use
     * persistent settings to update your cluster settings instead.
     */
    @Deprecated
    public ClusterUpdateSettingsRequest transientSettings(String source, XContentType xContentType) {
        this.transientSettings = Settings.builder().loadFromSource(source, xContentType).build();
        return this;
    }

    /**
     * Sets the transient settings to be updated. They will not survive a full cluster restart
     *
     * @deprecated Transient settings are in the process of being removed. Use
     * persistent settings to update your cluster settings instead.
     */
    @Deprecated
    public ClusterUpdateSettingsRequest transientSettings(Map<String, ?> source) {
        this.transientSettings = Settings.builder().loadFromMap(source).build();
        return this;
    }

    /**
     * Sets the persistent settings to be updated. They will get applied cross restarts
     */
    public ClusterUpdateSettingsRequest persistentSettings(Settings settings) {
        this.persistentSettings = settings;
        return this;
    }

    /**
     * Sets the persistent settings to be updated. They will get applied cross restarts
     */
    public ClusterUpdateSettingsRequest persistentSettings(Settings.Builder settings) {
        this.persistentSettings = settings.build();
        return this;
    }

    /**
     * Sets the persistent settings to be updated. They will get applied cross restarts
     */
    public ClusterUpdateSettingsRequest persistentSettings(Map<String, ?> source) {
        this.persistentSettings = Settings.builder().loadFromMap(source).build();
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        transientSettings.writeTo(out);
        persistentSettings.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(PERSISTENT.getPreferredName());
        persistentSettings.toXContent(builder, params);
        builder.endObject();
        builder.startObject(TRANSIENT.getPreferredName());
        transientSettings.toXContent(builder, params);
        builder.endObject();
        builder.endObject();
        return builder;
    }

    public static ClusterUpdateSettingsRequest fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }
}
