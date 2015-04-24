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

package org.elasticsearch.index.query;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Scopes;
import org.elasticsearch.common.inject.assistedinject.FactoryProvider;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.support.InnerHitsQueryParserHelper;

import java.util.LinkedList;
import java.util.Map;

/**
 *
 */
public class IndexQueryParserModule extends AbstractModule {

    /**
     * A custom processor that can be extended to process and bind custom implementations of
     * {@link QueryParserFactory}, and {@link FilterParser}.
     */
    public static class QueryParsersProcessor {

        /**
         * Extension point to bind a custom {@link QueryParserFactory}.
         */
        public void processXContentQueryParsers(XContentQueryParsersBindings bindings) {

        }

        public static class XContentQueryParsersBindings {

            private final MapBinder<String, QueryParserFactory> binder;
            private final Map<String, Settings> groupSettings;

            public XContentQueryParsersBindings(MapBinder<String, QueryParserFactory> binder, Map<String, Settings> groupSettings) {
                this.binder = binder;
                this.groupSettings = groupSettings;
            }

            public MapBinder<String, QueryParserFactory> binder() {
                return binder;
            }

            public Map<String, Settings> groupSettings() {
                return groupSettings;
            }

            public void processXContentQueryParser(String name, Class<? extends QueryParser> xcontentQueryParser) {
                if (!groupSettings.containsKey(name)) {
                    binder.addBinding(name).toProvider(FactoryProvider.newFactory(QueryParserFactory.class, xcontentQueryParser)).in(Scopes.SINGLETON);
                }
            }
        }

        /**
         * Extension point to bind a custom {@link FilterParserFactory}.
         */
        public void processXContentFilterParsers(XContentFilterParsersBindings bindings) {

        }

        public static class XContentFilterParsersBindings {

            private final MapBinder<String, FilterParserFactory> binder;
            private final Map<String, Settings> groupSettings;

            public XContentFilterParsersBindings(MapBinder<String, FilterParserFactory> binder, Map<String, Settings> groupSettings) {
                this.binder = binder;
                this.groupSettings = groupSettings;
            }

            public MapBinder<String, FilterParserFactory> binder() {
                return binder;
            }

            public Map<String, Settings> groupSettings() {
                return groupSettings;
            }

            public void processXContentQueryFilter(String name, Class<? extends FilterParser> xcontentFilterParser) {
                if (!groupSettings.containsKey(name)) {
                    binder.addBinding(name).toProvider(FactoryProvider.newFactory(FilterParserFactory.class, xcontentFilterParser)).in(Scopes.SINGLETON);
                }
            }
        }
    }

    private final Settings settings;

    private final LinkedList<QueryParsersProcessor> processors = Lists.newLinkedList();

    private final Map<String, Class<? extends QueryParser>> queries = Maps.newHashMap();
    private final Map<String, Class<? extends FilterParser>> filters = Maps.newHashMap();

    public IndexQueryParserModule(Settings settings) {
        this.settings = settings;
    }

    /**
     * Adds a custom query parser.
     *
     * @param name        The name of the query parser
     * @param queryParser the class of the query parser
     */
    public void addQueryParser(String name, Class<? extends QueryParser> queryParser) {
        queries.put(name, queryParser);
    }

    /**
     * Adds a custom filter parser.
     *
     * @param name         The name of the filter parser
     * @param filterParser the class of the filter parser
     */
    public void addFilterParser(String name, Class<? extends FilterParser> filterParser) {
        filters.put(name, filterParser);
    }

    public IndexQueryParserModule addProcessor(QueryParsersProcessor processor) {
        processors.addFirst(processor);
        return this;
    }

    @Override
    protected void configure() {

        bind(IndexQueryParserService.class).asEagerSingleton();
        bind(InnerHitsQueryParserHelper.class).asEagerSingleton();

        // handle XContenQueryParsers
        MapBinder<String, QueryParserFactory> queryBinder
                = MapBinder.newMapBinder(binder(), String.class, QueryParserFactory.class);
        Map<String, Settings> xContentQueryParserGroups = settings.getGroups(IndexQueryParserService.Defaults.QUERY_PREFIX);
        for (Map.Entry<String, Settings> entry : xContentQueryParserGroups.entrySet()) {
            String qName = entry.getKey();
            Settings qSettings = entry.getValue();
            Class<? extends QueryParser> type = qSettings.getAsClass("type", null);
            if (type == null) {
                throw new IllegalArgumentException("Query Parser [" + qName + "] must be provided with a type");
            }
            queryBinder.addBinding(qName).toProvider(FactoryProvider.newFactory(QueryParserFactory.class,
                    qSettings.getAsClass("type", null))).in(Scopes.SINGLETON);
        }

        QueryParsersProcessor.XContentQueryParsersBindings xContentQueryParsersBindings = new QueryParsersProcessor.XContentQueryParsersBindings(queryBinder, xContentQueryParserGroups);
        for (QueryParsersProcessor processor : processors) {
            processor.processXContentQueryParsers(xContentQueryParsersBindings);
        }

        for (Map.Entry<String, Class<? extends QueryParser>> entry : queries.entrySet()) {
            queryBinder.addBinding(entry.getKey()).toProvider(FactoryProvider.newFactory(QueryParserFactory.class, entry.getValue())).in(Scopes.SINGLETON);
        }

        // handle XContentFilterParsers
        MapBinder<String, FilterParserFactory> filterBinder
                = MapBinder.newMapBinder(binder(), String.class, FilterParserFactory.class);
        Map<String, Settings> xContentFilterParserGroups = settings.getGroups(IndexQueryParserService.Defaults.FILTER_PREFIX);
        for (Map.Entry<String, Settings> entry : xContentFilterParserGroups.entrySet()) {
            String fName = entry.getKey();
            Settings fSettings = entry.getValue();
            Class<? extends FilterParser> type = fSettings.getAsClass("type", null);
            if (type == null) {
                throw new IllegalArgumentException("Filter Parser [" + fName + "] must be provided with a type");
            }
            filterBinder.addBinding(fName).toProvider(FactoryProvider.newFactory(FilterParserFactory.class,
                    fSettings.getAsClass("type", null))).in(Scopes.SINGLETON);
        }

        QueryParsersProcessor.XContentFilterParsersBindings xContentFilterParsersBindings = new QueryParsersProcessor.XContentFilterParsersBindings(filterBinder, xContentFilterParserGroups);
        for (QueryParsersProcessor processor : processors) {
            processor.processXContentFilterParsers(xContentFilterParsersBindings);
        }

        for (Map.Entry<String, Class<? extends FilterParser>> entry : filters.entrySet()) {
            filterBinder.addBinding(entry.getKey()).toProvider(FactoryProvider.newFactory(FilterParserFactory.class, entry.getValue())).in(Scopes.SINGLETON);
        }
    }
}
