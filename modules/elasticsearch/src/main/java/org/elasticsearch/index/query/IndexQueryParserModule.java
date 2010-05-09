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

import org.elasticsearch.index.query.xcontent.*;
import org.elasticsearch.util.inject.AbstractModule;
import org.elasticsearch.util.inject.Scopes;
import org.elasticsearch.util.inject.assistedinject.FactoryProvider;
import org.elasticsearch.util.inject.multibindings.MapBinder;
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
                    qSettings.getAsClass("type", XContentIndexQueryParser.class))).in(Scopes.SINGLETON);
        }
        if (!queryParserGroupSettings.containsKey(IndexQueryParserService.Defaults.DEFAULT)) {
            qbinder.addBinding(IndexQueryParserService.Defaults.DEFAULT).toProvider(FactoryProvider.newFactory(IndexQueryParserFactory.class,
                    XContentIndexQueryParser.class)).in(Scopes.SINGLETON);
        }

        // handle XContenQueryParsers
        MapBinder<String, XContentQueryParserFactory> queryBinder
                = MapBinder.newMapBinder(binder(), String.class, XContentQueryParserFactory.class);
        Map<String, Settings> xContentQueryParserGroups = settings.getGroups(XContentIndexQueryParser.Defaults.QUERY_PREFIX);
        for (Map.Entry<String, Settings> entry : xContentQueryParserGroups.entrySet()) {
            String qName = entry.getKey();
            Settings qSettings = entry.getValue();
            Class<? extends XContentQueryParser> type = qSettings.getAsClass("type", null);
            if (type == null) {
                throw new IllegalArgumentException("Query Parser [" + qName + "] must be provided with a type");
            }
            queryBinder.addBinding(qName).toProvider(FactoryProvider.newFactory(XContentQueryParserFactory.class,
                    qSettings.getAsClass("type", null))).in(Scopes.SINGLETON);
        }

        // handle XContentFilterParsers
        MapBinder<String, XContentFilterParserFactory> filterBinder
                = MapBinder.newMapBinder(binder(), String.class, XContentFilterParserFactory.class);
        Map<String, Settings> xContentFilterParserGroups = settings.getGroups(XContentIndexQueryParser.Defaults.FILTER_PREFIX);
        for (Map.Entry<String, Settings> entry : xContentFilterParserGroups.entrySet()) {
            String fName = entry.getKey();
            Settings fSettings = entry.getValue();
            Class<? extends XContentFilterParser> type = fSettings.getAsClass("type", null);
            if (type == null) {
                throw new IllegalArgumentException("Filter Parser [" + fName + "] must be provided with a type");
            }
            filterBinder.addBinding(fName).toProvider(FactoryProvider.newFactory(XContentFilterParserFactory.class,
                    fSettings.getAsClass("type", null))).in(Scopes.SINGLETON);
        }

        bind(IndexQueryParserService.class).asEagerSingleton();
    }
}
