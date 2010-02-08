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

package org.elasticsearch.index.query;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.assistedinject.FactoryProvider;
import com.google.inject.multibindings.MapBinder;
import org.elasticsearch.index.query.json.*;
import org.elasticsearch.util.settings.Settings;

import java.util.Map;

/**
 * @author kimchy (Shay Banon)
 */
public class IndexQueryParserModule extends AbstractModule {

    private final Settings settings;

    public IndexQueryParserModule(Settings settings) {
        this.settings = settings;
    }

    @Override protected void configure() {

        // handle IndexQueryParsers
        MapBinder<String, IndexQueryParserFactory> qbinder
                = MapBinder.newMapBinder(binder(), String.class, IndexQueryParserFactory.class);

        Map<String, Settings> queryParserGroupSettings = settings.getGroups(IndexQueryParserService.Defaults.PREFIX);
        for (Map.Entry<String, Settings> entry : queryParserGroupSettings.entrySet()) {
            String qName = entry.getKey();
            Settings qSettings = entry.getValue();
            qbinder.addBinding(qName).toProvider(FactoryProvider.newFactory(IndexQueryParserFactory.class,
                    qSettings.getAsClass("type", JsonIndexQueryParser.class))).in(Scopes.SINGLETON);
        }
        if (!queryParserGroupSettings.containsKey(IndexQueryParserService.Defaults.DEFAULT)) {
            qbinder.addBinding(IndexQueryParserService.Defaults.DEFAULT).toProvider(FactoryProvider.newFactory(IndexQueryParserFactory.class,
                    JsonIndexQueryParser.class)).in(Scopes.SINGLETON);
        }

        // handle JsonQueryParsers
        MapBinder<String, JsonQueryParserFactory> jsonQueryBinder
                = MapBinder.newMapBinder(binder(), String.class, JsonQueryParserFactory.class);
        Map<String, Settings> jsonQueryParserGroups = settings.getGroups(JsonIndexQueryParser.Defaults.JSON_QUERY_PREFIX);
        for (Map.Entry<String, Settings> entry : jsonQueryParserGroups.entrySet()) {
            String qName = entry.getKey();
            Settings qSettings = entry.getValue();
            Class<? extends JsonQueryParser> type = qSettings.getAsClass("type", null);
            if (type == null) {
                throw new IllegalArgumentException("Json Query Parser [" + qName + "] must be provided with a type");
            }
            jsonQueryBinder.addBinding(qName).toProvider(FactoryProvider.newFactory(JsonQueryParserFactory.class,
                    qSettings.getAsClass("type", null))).in(Scopes.SINGLETON);
        }

        // handle JsonFilterParsers
        MapBinder<String, JsonFilterParserFactory> jsonFilterBinder
                = MapBinder.newMapBinder(binder(), String.class, JsonFilterParserFactory.class);
        Map<String, Settings> jsonFilterParserGroups = settings.getGroups(JsonIndexQueryParser.Defaults.JSON_FILTER_PREFIX);
        for (Map.Entry<String, Settings> entry : jsonFilterParserGroups.entrySet()) {
            String fName = entry.getKey();
            Settings fSettings = entry.getValue();
            Class<? extends JsonFilterParser> type = fSettings.getAsClass("type", null);
            if (type == null) {
                throw new IllegalArgumentException("Json Filter Parser [" + fName + "] must be provided with a type");
            }
            jsonFilterBinder.addBinding(fName).toProvider(FactoryProvider.newFactory(JsonFilterParserFactory.class,
                    fSettings.getAsClass("type", null))).in(Scopes.SINGLETON);
        }

        bind(IndexQueryParserService.class).asEagerSingleton();
    }
}
