/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.settings.put;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.settings.Settings.readSettingsFromStream;

/**
 * Request for an update index settings action
 */
public class UpdateSettingsRequest extends AcknowledgedRequest<UpdateSettingsRequest>
    implements
        IndicesRequest.Replaceable,
        ToXContentObject {

    public static final IndicesOptions DEFAULT_INDICES_OPTIONS = IndicesOptions.fromOptions(false, false, true, true);

    private String[] indices;
    private IndicesOptions indicesOptions = DEFAULT_INDICES_OPTIONS;
    private Settings settings = Settings.EMPTY;
    private boolean preserveExisting = false;
    private String origin = "";
    private boolean reopen = false;

    public UpdateSettingsRequest(StreamInput in) throws IOException {
        super(in);
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        settings = readSettingsFromStream(in);
        preserveExisting = in.readBoolean();
        origin = in.readString();
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            reopen = in.readBoolean();
        }
    }

    public UpdateSettingsRequest() {
        super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT, DEFAULT_ACK_TIMEOUT);
    }

    /**
     * Constructs a new request to update settings for one or more indices
     */
    public UpdateSettingsRequest(String... indices) {
        super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT, DEFAULT_ACK_TIMEOUT);
        this.indices = indices;
    }

    /**
     * Constructs a new request to update settings for one or more indices
     */
    public UpdateSettingsRequest(Settings settings, String... indices) {
        super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT, DEFAULT_ACK_TIMEOUT);
        this.indices = indices;
        this.settings = settings;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (settings.isEmpty()) {
            validationException = addValidationError("no settings to update", validationException);
        }
        return validationException;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    public Settings settings() {
        return settings;
    }

    /**
     * Sets the indices to apply to settings update to
     */
    @Override
    public UpdateSettingsRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    public UpdateSettingsRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    /**
     * Sets the settings to be updated
     */
    public UpdateSettingsRequest settings(Settings settings) {
        this.settings = settings;
        return this;
    }

    /**
     * Sets the settings to be updated
     */
    public UpdateSettingsRequest settings(Settings.Builder settings) {
        this.settings = settings.build();
        return this;
    }

    /**
     * Sets the settings to be updated (either json or yaml format)
     */
    public UpdateSettingsRequest settings(String source, XContentType xContentType) {
        this.settings = Settings.builder().loadFromSource(source, xContentType).build();
        return this;
    }

    /**
     * Returns <code>true</code> iff the settings update should only add but not update settings. If the setting already exists
     * it should not be overwritten by this update. The default is <code>false</code>
     */
    public boolean isPreserveExisting() {
        return preserveExisting;
    }

    /**
     * Iff set to <code>true</code> this settings update will only add settings not already set on an index. Existing settings remain
     * unchanged.
     */
    public UpdateSettingsRequest setPreserveExisting(boolean preserveExisting) {
        this.preserveExisting = preserveExisting;
        return this;
    }

    /**
     * Sets the settings to be updated (either json or yaml format)
     */
    public UpdateSettingsRequest settings(Map<String, ?> source) {
        this.settings = Settings.builder().loadFromMap(source).build();
        return this;
    }

    public String origin() {
        return origin;
    }

    public UpdateSettingsRequest origin(String origin) {
        this.origin = Objects.requireNonNull(origin, "origin cannot be null");
        return this;
    }

    /**
     * Returns <code>true</code> if non-dynamic setting updates should go through, by automatically unassigning shards in the same cluster
     * state change as the setting update. The shards will be automatically reassigned after the cluster state update is made. The
     * default is <code>false</code>.
     */
    public boolean reopen() {
        return reopen;
    }

    public void reopen(boolean reopen) {
        this.reopen = reopen;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArrayNullable(indices);
        indicesOptions.writeIndicesOptions(out);
        settings.writeTo(out);
        out.writeBoolean(preserveExisting);
        out.writeString(origin);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            out.writeBoolean(reopen);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        settings.toXContent(builder, params);
        builder.endObject();
        return builder;
    }

    public UpdateSettingsRequest fromXContent(XContentParser parser) throws IOException {
        Map<String, Object> settings = new HashMap<>();
        Map<String, Object> bodySettings = parser.map();
        Object innerBodySettings = bodySettings.get("settings");
        // clean up in case the body is wrapped with "settings" : { ... }
        if (innerBodySettings instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> innerBodySettingsMap = (Map<String, Object>) innerBodySettings;
            settings.putAll(innerBodySettingsMap);
            checkMixedRequest(bodySettings);
        } else {
            settings.putAll(bodySettings);
        }
        return this.settings(settings);
    }

    /**
     * Checks if the request is a "mixed request". A mixed request contains both a
     * "settings" map and "other" properties. Detection of a mixed request
     * will result in a parse exception being thrown.
     */
    private static void checkMixedRequest(Map<String, Object> bodySettings) {
        assert bodySettings.containsKey("settings");
        if (bodySettings.size() > 1) {
            throw new ElasticsearchParseException("mix of settings map and top-level properties");
        }
    }

    @Override
    public String toString() {
        return "indices : " + Arrays.toString(indices) + "," + Strings.toString(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UpdateSettingsRequest that = (UpdateSettingsRequest) o;
        return masterNodeTimeout().equals(that.masterNodeTimeout())
            && ackTimeout().equals(that.ackTimeout())
            && Objects.equals(settings, that.settings)
            && Objects.equals(indicesOptions, that.indicesOptions)
            && Objects.equals(preserveExisting, that.preserveExisting)
            && Objects.equals(reopen, that.reopen)
            && Arrays.equals(indices, that.indices);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            masterNodeTimeout(),
            ackTimeout(),
            settings,
            indicesOptions,
            preserveExisting,
            reopen,
            Arrays.hashCode(indices)
        );
    }

}
