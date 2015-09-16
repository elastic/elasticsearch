/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.settings.put;

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.settings.Settings.Builder.EMPTY_SETTINGS;
import static org.elasticsearch.common.settings.Settings.readSettingsFromStream;
import static org.elasticsearch.common.settings.Settings.writeSettingsToStream;

/**
 * Request for an update index settings action
 */
public class UpdateSettingsRequest extends AcknowledgedRequest<UpdateSettingsRequest> implements IndicesRequest.Replaceable {

    private String[] indices;
    private IndicesOptions indicesOptions = IndicesOptions.fromOptions(false, false, true, true);
    private Settings settings = EMPTY_SETTINGS;

    public UpdateSettingsRequest() {
    }

    /**
     * Constructs a new request to update settings for one or more indices
     */
    public UpdateSettingsRequest(String... indices) {
        this.indices = indices;
    }

    /**
     * Constructs a new request to update settings for one or more indices
     */
    public UpdateSettingsRequest(Settings settings, String... indices) {
        this.indices = indices;
        this.settings = settings;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (settings.getAsMap().isEmpty()) {
            validationException = addValidationError("no settings to update", validationException);
        }
        return validationException;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    Settings settings() {
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
     * Sets the settings to be updated (either json/yaml/properties format)
     */
    public UpdateSettingsRequest settings(String source) {
        this.settings = Settings.settingsBuilder().loadFromSource(source).build();
        return this;
    }

    /**
     * Sets the settings to be updated (either json/yaml/properties format)
     */
    @SuppressWarnings("unchecked")
    public UpdateSettingsRequest settings(Map source) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.map(source);
            settings(builder.string());
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        settings = readSettingsFromStream(in);
        readTimeout(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArrayNullable(indices);
        indicesOptions.writeIndicesOptions(out);
        writeSettingsToStream(settings, out);
        writeTimeout(out);
    }
}
