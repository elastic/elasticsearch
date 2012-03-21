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

package org.elasticsearch.index.source;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;

import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;

/**
 *
 */
public class SourceProviderService extends AbstractIndexComponent {

    public static class Defaults  {
        public static final String SOURCE_PREFIX = "index.source.provider";
    }

    private final Map<String, SourceProviderParser> sourceProviderParsers;

    @Inject
    public SourceProviderService(Index index, @IndexSettings Settings indexSettings,
                                 @Nullable Map<String, SourceProviderParserFactory> sourceProviderParserFactories) {
        super(index, indexSettings);

        Map<String, SourceProviderParser> sourceProviderMap = newHashMap();
        if (sourceProviderParserFactories != null) {
            Map<String, Settings> sourceProvidersSettings = indexSettings.getGroups("index.source.provider");
            for (Map.Entry<String, SourceProviderParserFactory> entry : sourceProviderParserFactories.entrySet()) {
                String sourceProviderName = entry.getKey();
                SourceProviderParserFactory sourceProviderParserFactory = entry.getValue();

                Settings sourceProviderSettings = sourceProvidersSettings.get(sourceProviderName);
                if (sourceProviderSettings == null) {
                    sourceProviderSettings = ImmutableSettings.Builder.EMPTY_SETTINGS;
                }

                SourceProviderParser sourceProviderParser = sourceProviderParserFactory.create(sourceProviderName, sourceProviderSettings);
                sourceProviderMap.put(sourceProviderName, sourceProviderParser);
                sourceProviderMap.put(Strings.toCamelCase(sourceProviderName), sourceProviderParser);
            }
        }
        this.sourceProviderParsers = ImmutableMap.copyOf(sourceProviderMap);
    }

    
    public SourceProviderParser sourceProviderParser(String name) {
        return sourceProviderParsers.get(name);
    }
}
