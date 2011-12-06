/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.common.settings.loader;

import org.elasticsearch.common.io.FastByteArrayInputStream;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;

/**
 * Settings loader that loads (parses) the settings in a yaml format by flattening them
 * into a map.
 *
 *
 */
public class YamlSettingsLoader implements SettingsLoader {

    @Override
    public Map<String, String> load(String source) throws IOException {
        // replace tabs with whitespace (yaml does not accept tabs, but many users might use it still...)
        source = source.replace("\t", "  ");
        Yaml yaml = new Yaml();
        Map<Object, Object> yamlMap = (Map<Object, Object>) yaml.load(source);
        StringBuilder sb = new StringBuilder();
        Map<String, String> settings = newHashMap();
        if (yamlMap == null) {
            return settings;
        }
        List<String> path = newArrayList();
        serializeMap(settings, sb, path, yamlMap);
        return settings;
    }

    @Override
    public Map<String, String> load(byte[] source) throws IOException {
        Yaml yaml = new Yaml();
        Map<Object, Object> yamlMap = (Map<Object, Object>) yaml.load(new FastByteArrayInputStream(source));
        StringBuilder sb = new StringBuilder();
        Map<String, String> settings = newHashMap();
        if (yamlMap == null) {
            return settings;
        }
        List<String> path = newArrayList();
        serializeMap(settings, sb, path, yamlMap);
        return settings;
    }

    private void serializeMap(Map<String, String> settings, StringBuilder sb, List<String> path, Map<Object, Object> yamlMap) {
        for (Map.Entry<Object, Object> entry : yamlMap.entrySet()) {
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

    private void serializeList(Map<String, String> settings, StringBuilder sb, List<String> path, List yamlList) {
        int counter = 0;
        for (Object listEle : yamlList) {
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

    private void serializeValue(Map<String, String> settings, StringBuilder sb, List<String> path, String name, Object value) {
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
