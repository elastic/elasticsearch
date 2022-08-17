/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.settings.get;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class GetSettingsResponse extends ActionResponse implements ToXContentObject {

    private final Map<String, Settings> indexToSettings;
    private final Map<String, Settings> indexToDefaultSettings;

    public GetSettingsResponse(Map<String, Settings> indexToSettings, Map<String, Settings> indexToDefaultSettings) {
        this.indexToSettings = indexToSettings;
        this.indexToDefaultSettings = indexToDefaultSettings;
    }

    public GetSettingsResponse(StreamInput in) throws IOException {
        super(in);
        indexToSettings = in.readImmutableMap(StreamInput::readString, Settings::readSettingsFromStream);
        indexToDefaultSettings = in.readImmutableMap(StreamInput::readString, Settings::readSettingsFromStream);
    }

    /**
     * Returns a map of index name to {@link Settings} object.  The returned {@link Settings}
     * objects contain only those settings explicitly set on a given index.  Any settings
     * taking effect as defaults must be accessed via {@link #getIndexToDefaultSettings()}.
     */
    public Map<String, Settings> getIndexToSettings() {
        return indexToSettings;
    }

    /**
     * If the originating {@link GetSettingsRequest} object was configured to include
     * defaults, this will contain a mapping of index name to {@link Settings} objects.
     * The returned {@link Settings} objects will contain only those settings taking
     * effect as defaults.  Any settings explicitly set on the index will be available
     * via {@link #getIndexToSettings()}.
     * See also {@link GetSettingsRequest#includeDefaults(boolean)}
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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(indexToSettings, StreamOutput::writeString, (o, v) -> v.writeTo(o));
        out.writeMap(indexToDefaultSettings, StreamOutput::writeString, (o, v) -> v.writeTo(o));
    }

    private static void parseSettingsField(
        XContentParser parser,
        String currentIndexName,
        Map<String, Settings> indexToSettings,
        Map<String, Settings> indexToDefaultSettings
    ) throws IOException {

        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            switch (parser.currentName()) {
                case "settings" -> indexToSettings.put(currentIndexName, Settings.fromXContent(parser));
                case "defaults" -> indexToDefaultSettings.put(currentIndexName, Settings.fromXContent(parser));
                default -> parser.skipChildren();
            }
        } else if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
            parser.skipChildren();
        }
        parser.nextToken();
    }

    private static void parseIndexEntry(
        XContentParser parser,
        Map<String, Settings> indexToSettings,
        Map<String, Settings> indexToDefaultSettings
    ) throws IOException {
        String indexName = parser.currentName();
        parser.nextToken();
        while (parser.isClosed() == false && parser.currentToken() != XContentParser.Token.END_OBJECT) {
            parseSettingsField(parser, indexName, indexToSettings, indexToDefaultSettings);
        }
    }

    public static GetSettingsResponse fromXContent(XContentParser parser) throws IOException {
        HashMap<String, Settings> indexToSettings = new HashMap<>();
        HashMap<String, Settings> indexToDefaultSettings = new HashMap<>();

        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        parser.nextToken();

        while (parser.isClosed() == false) {
            if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                // we must assume this is an index entry
                parseIndexEntry(parser, indexToSettings, indexToDefaultSettings);
            } else if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                parser.skipChildren();
            } else {
                parser.nextToken();
            }
        }

        return new GetSettingsResponse(Map.copyOf(indexToSettings), Map.copyOf(indexToDefaultSettings));
    }

    @Override
    public String toString() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            XContentBuilder builder = new XContentBuilder(JsonXContent.jsonXContent, baos);
            toXContent(builder, ToXContent.EMPTY_PARAMS, false);
            return Strings.toString(builder);
        } catch (IOException e) {
            throw new IllegalStateException(e); // should not be possible here
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return toXContent(builder, params, indexToDefaultSettings.isEmpty());
    }

    private XContentBuilder toXContent(XContentBuilder builder, Params params, boolean omitEmptySettings) throws IOException {
        builder.startObject();
        for (Map.Entry<String, Settings> cursor : getIndexToSettings().entrySet()) {
            // no settings, jump over it to shorten the response data
            if (omitEmptySettings && cursor.getValue().isEmpty()) {
                continue;
            }
            builder.startObject(cursor.getKey());
            builder.startObject("settings");
            cursor.getValue().toXContent(builder, params);
            builder.endObject();
            if (indexToDefaultSettings.isEmpty() == false) {
                builder.startObject("defaults");
                indexToDefaultSettings.get(cursor.getKey()).toXContent(builder, params);
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
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
