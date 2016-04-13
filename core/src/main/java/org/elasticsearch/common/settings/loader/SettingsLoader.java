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

package org.elasticsearch.common.settings.loader;

import org.elasticsearch.common.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Provides the ability to load settings (in the form of a simple Map) from
 * the actual source content that represents them.
 */
public interface SettingsLoader {

    static class Helper {

        public static Map<String, String> loadNestedFromMap(@Nullable Map map) {
            Map<String, String> settings = new HashMap<>();
            if (map == null) {
                return settings;
            }
            StringBuilder sb = new StringBuilder();
            List<String> path = new ArrayList<>();
            serializeMap(settings, sb, path, map);
            return settings;
        }

        private static void serializeMap(Map<String, String> settings, StringBuilder sb, List<String> path, Map<Object, Object> map) {
            for (Map.Entry<Object, Object> entry : map.entrySet()) {
                if (entry.getValue() instanceof Map) {
                    path.add((String) entry.getKey());
                    serializeMap(settings, sb, path, (Map<Object, Object>) entry.getValue());
                    path.remove(path.size() - 1);
                } else if (entry.getValue() instanceof List) {
                    path.add((String) entry.getKey());
                    serializeList(settings, sb, path, (List) entry.getValue());
                    path.remove(path.size() - 1);
                } else {
                    serializeValue(settings, sb, path, (String) entry.getKey(), entry.getValue());
                }
            }
        }

        private static void serializeList(Map<String, String> settings, StringBuilder sb, List<String> path, List list) {
            int counter = 0;
            for (Object listEle : list) {
                if (listEle instanceof Map) {
                    path.add(Integer.toString(counter));
                    serializeMap(settings, sb, path, (Map<Object, Object>) listEle);
                    path.remove(path.size() - 1);
                } else if (listEle instanceof List) {
                    path.add(Integer.toString(counter));
                    serializeList(settings, sb, path, (List) listEle);
                    path.remove(path.size() - 1);
                } else {
                    serializeValue(settings, sb, path, Integer.toString(counter), listEle);
                }
                counter++;
            }
        }

        private static void serializeValue(Map<String, String> settings, StringBuilder sb, List<String> path, String name, Object value) {
            if (value == null) {
                return;
            }
            sb.setLength(0);
            for (String pathEle : path) {
                sb.append(pathEle).append('.');
            }
            sb.append(name);
            settings.put(sb.toString(), value.toString());
        }
    }


    /**
     * Loads (parses) the settings from a source string.
     */
    Map<String, String> load(String source) throws IOException;

    /**
     * Loads (parses) the settings from a source bytes.
     */
    Map<String, String> load(byte[] source) throws IOException;
}
