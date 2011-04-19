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

import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Scopes;
import org.elasticsearch.common.inject.assistedinject.FactoryProvider;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.xcontent.*;

import java.util.LinkedList;
import java.util.Map;

/**
 * @author kimchy (Shay Banon)
 */
public class IndexQueryParserModule extends AbstractModule {

    /**
     * A custom processor that can be extended to process and bind custom implementations of
     * {@link IndexQueryParserFactory}, {@link XContentQueryParserFactory}, and {@link XContentFilterParser}.
     */
    public static class QueryParsersProcessor {

        /**
         * Extension point to bind a custom {@link IndexQueryParserFactory}.
         */
        public void processIndexQueryParsers(IndexQueryParsersBindings bindings) {

        }

        public static class IndexQueryParsersBindings {
            private final MapBinder<String, IndexQueryParserFactory> binder;
            private final Map<String, Settings> groupSettings;

            public IndexQueryParsersBindings(MapBinder<String, IndexQueryParserFactory> binder, Map<String, Settings> groupSettings) {
                this.binder = binder;
                this.groupSettings = groupSettings;
            }

            public MapBinder<String, IndexQueryParserFactory> binder() {
                return binder;
            }

            public Map<String, Settings> groupSettings() {
                return groupSettings;
            }

            public void processIndexQueryParser(String name, Class<? extends IndexQueryParser> indexQueryParser) {
                if (!groupSettings.containsKey(name)) {
                    binder.addBinding(name).toProvider(FactoryProvider.newFactory(IndexQueryParserFactory.class, indexQueryParser)).in(Scopes.SINGLETON);
                }
            }
        }

        /**
         * Extension point to bind a custom {@link XContentQueryParserFactory}.
         */
        public void processXContentQueryParsers(XContentQueryParsersBindings bindings) {

        }

        public static class XContentQueryParsersBindings {

            private final MapBinder<String, XContentQueryParserFactory> binder;
            private final Map<String, Settings> groupSettings;

            public XContentQueryParsersBindings(MapBinder<String, XContentQueryParserFactory> binder, Map<String, Settings> groupSettings) {
                this.binder = binder;
                this.groupSettings = groupSettings;
            }

            public MapBinder<String, XContentQueryParserFactory> binder() {
                return binder;
            }

            public Map<String, Settings> groupSettings() {
                return groupSettings;
            }

            public void processXContentQueryParser(String name, Class<? extends XContentQueryParser> xcontentQueryParser) {
                if (!groupSettings.containsKey(name)) {
                    binder.addBinding(name).toProvider(FactoryProvider.newFactory(XContentQueryParserFactory.class, xcontentQueryParser)).in(Scopes.SINGLETON);
                }
            }
        }

        /**
         * Extension point to bind a custom {@link XContentFilterParserFactory}.
         */
        public void processXContentFilterParsers(XContentFilterParsersBindings bindings) {

        }

        public static class XContentFilterParsersBindings {

            private final MapBinder<String, XContentFilterParserFactory> binder;
            private final Map<String, Settings> groupSettings;

            public XContentFilterParsersBindings(MapBinder<String, XContentFilterParserFactory> binder, Map<String, Settings> groupSettings) {
                this.binder = binder;
                this.groupSettings = groupSettings;
            }

            public MapBinder<String, XContentFilterParserFactory> binder() {
                return binder;
            }

            public Map<String, Settings> groupSettings() {
                return groupSettings;
            }

            public void processXContentQueryFilter(String name, Class<? extends XContentFilterParser> xcontentFilterParser) {
                if (!groupSettings.containsKey(name)) {
                    binder.addBinding(name).toProvider(FactoryProvider.newFactory(XContentFilterParserFactory.class, xcontentFilterParser)).in(Scopes.SINGLETON);
                }
            }
        }
    }

    private final Settings settings;

    private final LinkedList<QueryParsersProcessor> processors = Lists.newLinkedList();


    public IndexQueryParserModule(Settings settings) {
        this.settings = settings;
        this.processors.add(new DefaultQueryProcessors());
    }

    public IndexQueryParserModule addProcessor(QueryParsersProcessor processor) {
        processors.addFirst(processor);
        return this;
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

        QueryParsersProcessor.IndexQueryParsersBindings queryParsersBindings = new QueryParsersProcessor.IndexQueryParsersBindings(qbinder, queryParserGroupSettings);
        for (QueryParsersProcessor processor : processors) {
            processor.processIndexQueryParsers(queryParsersBindings);
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

        QueryParsersProcessor.XContentQueryParsersBindings xContentQueryParsersBindings = new QueryParsersProcessor.XContentQueryParsersBindings(queryBinder, xContentQueryParserGroups);
        for (QueryParsersProcessor processor : processors) {
            processor.processXContentQueryParsers(xContentQueryParsersBindings);
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

        QueryParsersProcessor.XContentFilterParsersBindings xContentFilterParsersBindings = new QueryParsersProcessor.XContentFilterParsersBindings(filterBinder, xContentFilterParserGroups);
        for (QueryParsersProcessor processor : processors) {
            processor.processXContentFilterParsers(xContentFilterParsersBindings);
        }

        bind(IndexQueryParserService.class).asEagerSingleton();
    }

    private static class DefaultQueryProcessors extends QueryParsersProcessor {
        @Override public void processXContentQueryParsers(XContentQueryParsersBindings bindings) {
            bindings.processXContentQueryParser(HasChildQueryParser.NAME, HasChildQueryParser.class);
            bindings.processXContentQueryParser(TopChildrenQueryParser.NAME, TopChildrenQueryParser.class);
            bindings.processXContentQueryParser(DisMaxQueryParser.NAME, DisMaxQueryParser.class);
            bindings.processXContentQueryParser(IdsQueryParser.NAME, IdsQueryParser.class);
            bindings.processXContentQueryParser(MatchAllQueryParser.NAME, MatchAllQueryParser.class);
            bindings.processXContentQueryParser(QueryStringQueryParser.NAME, QueryStringQueryParser.class);
            bindings.processXContentQueryParser(BoostingQueryParser.NAME, BoostingQueryParser.class);
            bindings.processXContentQueryParser(BoolQueryParser.NAME, BoolQueryParser.class);
            bindings.processXContentQueryParser(TermQueryParser.NAME, TermQueryParser.class);
            bindings.processXContentQueryParser(TermsQueryParser.NAME, TermsQueryParser.class);
            bindings.processXContentQueryParser(FuzzyQueryParser.NAME, FuzzyQueryParser.class);
            bindings.processXContentQueryParser(FieldQueryParser.NAME, FieldQueryParser.class);
            bindings.processXContentQueryParser(RangeQueryParser.NAME, RangeQueryParser.class);
            bindings.processXContentQueryParser(PrefixQueryParser.NAME, PrefixQueryParser.class);
            bindings.processXContentQueryParser(WildcardQueryParser.NAME, WildcardQueryParser.class);
            bindings.processXContentQueryParser(FilteredQueryParser.NAME, FilteredQueryParser.class);
            bindings.processXContentQueryParser(ConstantScoreQueryParser.NAME, ConstantScoreQueryParser.class);
            bindings.processXContentQueryParser(CustomBoostFactorQueryParser.NAME, CustomBoostFactorQueryParser.class);
            bindings.processXContentQueryParser(CustomScoreQueryParser.NAME, CustomScoreQueryParser.class);
            bindings.processXContentQueryParser(SpanTermQueryParser.NAME, SpanTermQueryParser.class);
            bindings.processXContentQueryParser(SpanNotQueryParser.NAME, SpanNotQueryParser.class);
            bindings.processXContentQueryParser(SpanFirstQueryParser.NAME, SpanFirstQueryParser.class);
            bindings.processXContentQueryParser(SpanNearQueryParser.NAME, SpanNearQueryParser.class);
            bindings.processXContentQueryParser(SpanOrQueryParser.NAME, SpanOrQueryParser.class);
            bindings.processXContentQueryParser(MoreLikeThisQueryParser.NAME, MoreLikeThisQueryParser.class);
            bindings.processXContentQueryParser(MoreLikeThisFieldQueryParser.NAME, MoreLikeThisFieldQueryParser.class);
            bindings.processXContentQueryParser(FuzzyLikeThisQueryParser.NAME, FuzzyLikeThisQueryParser.class);
            bindings.processXContentQueryParser(FuzzyLikeThisFieldQueryParser.NAME, FuzzyLikeThisFieldQueryParser.class);
        }

        @Override public void processXContentFilterParsers(XContentFilterParsersBindings bindings) {
            bindings.processXContentQueryFilter(HasChildFilterParser.NAME, HasChildFilterParser.class);
            bindings.processXContentQueryFilter(TypeFilterParser.NAME, TypeFilterParser.class);
            bindings.processXContentQueryFilter(IdsFilterParser.NAME, IdsFilterParser.class);
            bindings.processXContentQueryFilter(TermFilterParser.NAME, TermFilterParser.class);
            bindings.processXContentQueryFilter(TermsFilterParser.NAME, TermsFilterParser.class);
            bindings.processXContentQueryFilter(RangeFilterParser.NAME, RangeFilterParser.class);
            bindings.processXContentQueryFilter(NumericRangeFilterParser.NAME, NumericRangeFilterParser.class);
            bindings.processXContentQueryFilter(PrefixFilterParser.NAME, PrefixFilterParser.class);
            bindings.processXContentQueryFilter(ScriptFilterParser.NAME, ScriptFilterParser.class);
            bindings.processXContentQueryFilter(GeoDistanceFilterParser.NAME, GeoDistanceFilterParser.class);
            bindings.processXContentQueryFilter(GeoDistanceRangeFilterParser.NAME, GeoDistanceRangeFilterParser.class);
            bindings.processXContentQueryFilter(GeoBoundingBoxFilterParser.NAME, GeoBoundingBoxFilterParser.class);
            bindings.processXContentQueryFilter(GeoPolygonFilterParser.NAME, GeoPolygonFilterParser.class);
            bindings.processXContentQueryFilter(QueryFilterParser.NAME, QueryFilterParser.class);
            bindings.processXContentQueryFilter(FQueryFilterParser.NAME, FQueryFilterParser.class);
            bindings.processXContentQueryFilter(BoolFilterParser.NAME, BoolFilterParser.class);
            bindings.processXContentQueryFilter(AndFilterParser.NAME, AndFilterParser.class);
            bindings.processXContentQueryFilter(OrFilterParser.NAME, OrFilterParser.class);
            bindings.processXContentQueryFilter(NotFilterParser.NAME, NotFilterParser.class);
            bindings.processXContentQueryFilter(MatchAllFilterParser.NAME, MatchAllFilterParser.class);
            bindings.processXContentQueryFilter(ExistsFilterParser.NAME, ExistsFilterParser.class);
            bindings.processXContentQueryFilter(MissingFilterParser.NAME, MissingFilterParser.class);
        }
    }
}
