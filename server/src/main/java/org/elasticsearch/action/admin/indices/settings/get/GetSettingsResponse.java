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

package org.elasticsearch.action.admin.indices.settings.get;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

public class GetSettingsResponse extends ActionResponse implements ToXContentObject {

    private ImmutableOpenMap<String, Settings> indexToSettings = ImmutableOpenMap.of();

    private static final ConstructingObjectParser<GetSettingsResponse, Void> PARSER = new ConstructingObjectParser<>(
        "get_index_settings_response", true, args -> new GetSettingsResponse()
    );

    public GetSettingsResponse(ImmutableOpenMap<String, Settings> indexToSettings) {
        this.indexToSettings = indexToSettings;
    }

    GetSettingsResponse() {
    }

    public ImmutableOpenMap<String, Settings> getIndexToSettings() {
        return indexToSettings;
    }

    public String getSetting(String index, String setting) {
        Settings settings = indexToSettings.get(index);
        if (setting != null) {
            return settings.get(setting);
        } else {
            return null;
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int size = in.readVInt();
        ImmutableOpenMap.Builder<String, Settings> builder = ImmutableOpenMap.builder();
        for (int i = 0; i < size; i++) {
            builder.put(in.readString(), Settings.readSettingsFromStream(in));
        }
        indexToSettings = builder.build();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(indexToSettings.size());
        for (ObjectObjectCursor<String, Settings> cursor : indexToSettings) {
            out.writeString(cursor.key);
            Settings.writeSettingsToStream(cursor.value, out);
        }
    }

    public static GetSettingsResponse fromXContent(XContentParser parser) throws IOException {
        HashMap<String, Settings> indexToSettings = new HashMap<>();
        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
        parser.nextToken();

        while (!parser.isClosed() && parser.currentToken() == XContentParser.Token.FIELD_NAME) {
            String indexName = parser.currentName();
            parser.nextToken(); //go to per-index settings object
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
            XContentParserUtils.ensureFieldName(parser, parser.nextToken(), "settings");
            parser.nextToken(); //go to settings object itself
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
            Settings settings = Settings.fromXContent(parser);
            parser.nextToken(); //end per-index object
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.currentToken(), parser::getTokenLocation);
            parser.nextToken(); //we should now be positioned at the beginning of the next index's settings, or at the end
            indexToSettings.put(indexName, settings);
        }
        return new GetSettingsResponse(ImmutableOpenMap.<String, Settings>builder().putAll(indexToSettings).build());
    }
    @Override
    public String toString() {
        return indexToSettings.toString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        Iterator<String> indicesIterator = indexToSettings.keysIt();
        while (indicesIterator.hasNext()) {
            String indexName = indicesIterator.next();
            Settings settings = indexToSettings.get(indexName);
            builder.field(indexName);
            builder.startObject();
            builder.field("settings");
            builder.startObject();
            settings.toXContent(builder, params);
            builder.endObject();
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
        return Objects.equals(indexToSettings, that.indexToSettings);
    }

    @Override
    public int hashCode() {

        return Objects.hash(indexToSettings);
    }
}
