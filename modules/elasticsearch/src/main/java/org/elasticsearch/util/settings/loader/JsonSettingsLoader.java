/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.util.settings.loader;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.elasticsearch.util.io.FastStringReader;
import org.elasticsearch.util.json.Jackson;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.*;
import static com.google.common.collect.Maps.*;

/**
 * Loads settings from json source. Basically, flats them into a Map.
 *
 * @author kimchy (Shay Banon)
 */
public class JsonSettingsLoader implements SettingsLoader {

    private final JsonFactory jsonFactory = Jackson.defaultJsonFactory();

    @Override public Map<String, String> load(String source) throws IOException {
        JsonParser jp = jsonFactory.createJsonParser(new FastStringReader(source));
        return load(jp);
    }

    public Map<String, String> load(JsonParser jp) throws IOException {
        StringBuilder sb = new StringBuilder();
        Map<String, String> settings = newHashMap();
        List<String> path = newArrayList();
        jp.nextToken();
        serializeObject(settings, sb, path, jp, null);
        return settings;
    }

    private void serializeObject(Map<String, String> settings, StringBuilder sb, List<String> path, JsonParser jp, String objFieldName) throws IOException {
        if (objFieldName != null) {
            path.add(objFieldName);
        }

        String currentFieldName = null;
        JsonToken token;
        while ((token = jp.nextToken()) != JsonToken.END_OBJECT) {
            if (token == JsonToken.START_OBJECT) {
                serializeObject(settings, sb, path, jp, currentFieldName);
            } else if (token == JsonToken.START_ARRAY) {
                serializeArray(settings, sb, path, jp, currentFieldName);
            } else if (token == JsonToken.FIELD_NAME) {
                currentFieldName = jp.getCurrentName();
            } else if (token == JsonToken.VALUE_NULL) {
                // ignore this
            } else {
                serializeValue(settings, sb, path, jp, currentFieldName);

            }
        }

        if (objFieldName != null) {
            path.remove(path.size() - 1);
        }
    }

    private void serializeArray(Map<String, String> settings, StringBuilder sb, List<String> path, JsonParser jp, String fieldName) throws IOException {
        JsonToken token;
        int counter = 0;
        while ((token = jp.nextToken()) != JsonToken.END_ARRAY) {
            if (token == JsonToken.START_OBJECT) {
                serializeObject(settings, sb, path, jp, fieldName + '.' + (counter++));
            } else if (token == JsonToken.START_ARRAY) {
                serializeArray(settings, sb, path, jp, fieldName + '.' + (counter++));
            } else if (token == JsonToken.FIELD_NAME) {
                fieldName = jp.getCurrentName();
            } else if (token == JsonToken.VALUE_NULL) {
                // ignore
            } else {
                serializeValue(settings, sb, path, jp, fieldName + '.' + (counter++));
            }
        }
    }

    private void serializeValue(Map<String, String> settings, StringBuilder sb, List<String> path, JsonParser jp, String fieldName) throws IOException {
        sb.setLength(0);
        for (String pathEle : path) {
            sb.append(pathEle).append('.');
        }
        sb.append(fieldName);
        settings.put(sb.toString(), jp.getText());
    }

}
