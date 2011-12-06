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

import java.io.IOException;
import java.util.Map;

/**
 * Provides the ability to load settings (in the form of a simple Map) from
 * the actual source content that represents them.
 *
 *
 */
public interface SettingsLoader {

    /**
     * Loads (parses) the settings from a source string.
     */
    Map<String, String> load(String source) throws IOException;

    /**
     * Loads (parses) the settings from a source bytes.
     */
    Map<String, String> load(byte[] source) throws IOException;
}
