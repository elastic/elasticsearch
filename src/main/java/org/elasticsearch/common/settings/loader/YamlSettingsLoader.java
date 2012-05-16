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
import java.util.Map;

/**
 * Settings loader that loads (parses) the settings in a yaml format by flattening them
 * into a map.
 */
public class YamlSettingsLoader implements SettingsLoader {

    @Override
    public Map<String, String> load(String source) throws IOException {
        // replace tabs with whitespace (yaml does not accept tabs, but many users might use it still...)
        source = source.replace("\t", "  ");
        Yaml yaml = new Yaml();
        Map<Object, Object> yamlMap = (Map<Object, Object>) yaml.load(source);
        return Helper.loadNestedFromMap(yamlMap);
    }

    @Override
    public Map<String, String> load(byte[] source) throws IOException {
        Yaml yaml = new Yaml();
        Map<Object, Object> yamlMap = (Map<Object, Object>) yaml.load(new FastByteArrayInputStream(source));
        return Helper.loadNestedFromMap(yamlMap);
    }
}
