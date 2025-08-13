/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.settings.get;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

public class GetSettingsResponse extends ActionResponse implements ChunkedToXContentObject {

    private final Map<String, Settings> indexToSettings;
    private final Map<String, Settings> indexToDefaultSettings;

    public GetSettingsResponse(Map<String, Settings> indexToSettings, Map<String, Settings> indexToDefaultSettings) {
        this.indexToSettings = indexToSettings;
        this.indexToDefaultSettings = indexToDefaultSettings;
    }

    /**
     * Returns a map of index name to {@link Settings} object.  The returned {@link Settings}
     * objects contain only those settings explicitly set on a given index.
     */
    public Map<String, Settings> getIndexToSettings() {
        return indexToSettings;
    }

    /**
     * Returns a map of index name to {@link Settings} object.  The returned {@link Settings}
     * objects contain only the default settings
     */
    public Map<String, Settings> getIndexToDefaultSettings() {
        return indexToDefaultSettings;
    }

    /**
     * Returns the string value for the specified index and setting.  If the includeDefaults
     * flag was not set or set to false on the GetSettingsRequest, this method will only
     * return a value where the setting was explicitly set on the index.  If the includeDefaults
     * flag was set to true on the GetSettingsRequest, this method will fall back to return the default
     * value if the setting was not explicitly set.
     */
    public String getSetting(String index, String setting) {
        Settings settings = indexToSettings.get(index);
        if (setting != null) {
            if (settings != null && settings.hasValue(setting)) {
                return settings.get(setting);
            } else {
                Settings defaultSettings = indexToDefaultSettings.get(index);
                if (defaultSettings != null) {
                    return defaultSettings.get(setting);
                } else {
                    return null;
                }
            }
        } else {
            return null;
        }
    }

    /**
     * NB prior to 9.1 this was a TransportMasterNodeReadAction so for BwC we must remain able to write these responses until
     * we no longer need to support calling this action remotely.
     */
    @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT)
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(indexToSettings, StreamOutput::writeWriteable);
        out.writeMap(indexToDefaultSettings, StreamOutput::writeWriteable);
    }

    @Override
    public String toString() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            XContentBuilder builder = new XContentBuilder(JsonXContent.jsonXContent, baos);
            var iterator = toXContentChunked(false);
            while (iterator.hasNext()) {
                iterator.next().toXContent(builder, ToXContent.EMPTY_PARAMS);
            }
            return Strings.toString(builder);
        } catch (IOException e) {
            throw new IllegalStateException(e); // should not be possible here
        }
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        final boolean omitEmptySettings = indexToDefaultSettings.isEmpty();
        return toXContentChunked(omitEmptySettings);
    }

    private Iterator<ToXContent> toXContentChunked(boolean omitEmptySettings) {
        final boolean indexToDefaultSettingsEmpty = indexToDefaultSettings.isEmpty();
        return Iterators.concat(
            Iterators.single((builder, params) -> builder.startObject()),
            getIndexToSettings().entrySet()
                .stream()
                .filter(entry -> omitEmptySettings == false || entry.getValue().isEmpty() == false)
                .map(entry -> (ToXContent) (builder, params) -> {
                    builder.startObject(entry.getKey());
                    builder.startObject("settings");
                    entry.getValue().toXContent(builder, params);
                    builder.endObject();
                    if (indexToDefaultSettingsEmpty == false) {
                        builder.startObject("defaults");
                        indexToDefaultSettings.get(entry.getKey()).toXContent(builder, params);
                        builder.endObject();
                    }
                    return builder.endObject();
                })
                .iterator(),
            Iterators.single((builder, params) -> builder.endObject())
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetSettingsResponse that = (GetSettingsResponse) o;
        return Objects.equals(indexToSettings, that.indexToSettings) && Objects.equals(indexToDefaultSettings, that.indexToDefaultSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexToSettings, indexToDefaultSettings);
    }
}
