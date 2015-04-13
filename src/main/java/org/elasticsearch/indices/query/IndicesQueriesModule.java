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

package org.elasticsearch.indices.query;

import com.google.common.collect.Sets;
import org.elasticsearch.common.geo.ShapesAvailability;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.Multibinder;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryParser;

import java.util.Set;

public class IndicesQueriesModule extends AbstractModule {

    private Set<Class<QueryParser>> queryParsersClasses = Sets.newHashSet();
    private Set<QueryParser> queryParsers = Sets.newHashSet();
    private Set<Class<FilterParser>> filterParsersClasses = Sets.newHashSet();
    private Set<FilterParser> filterParsers = Sets.newHashSet();

    public synchronized IndicesQueriesModule addQuery(Class<QueryParser> queryParser) {
        queryParsersClasses.add(queryParser);
        return this;
    }

    public synchronized IndicesQueriesModule addQuery(QueryParser queryParser) {
        queryParsers.add(queryParser);
        return this;
    }

    public synchronized IndicesQueriesModule addFilter(Class<FilterParser> filterParser) {
        filterParsersClasses.add(filterParser);
        return this;
    }

    public synchronized IndicesQueriesModule addFilter(FilterParser filterParser) {
        filterParsers.add(filterParser);
        return this;
    }

    @Override
    protected void configure() {
        bind(IndicesQueriesRegistry.class).asEagerSingleton();

        Multibinder<QueryParser> qpBinders = Multibinder.newSetBinder(binder(), QueryParser.class);
        for (Class<QueryParser> queryParser : queryParsersClasses) {
            qpBinders.addBinding().to(queryParser).asEagerSingleton();
        }
        for (QueryParser queryParser : queryParsers) {
            qpBinders.addBinding().toInstance(queryParser);
        }
        qpBinders.addBinding().to(MatchQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(MultiMatchQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(NestedQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(HasChildQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(HasParentQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(TopChildrenQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(DisMaxQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(IdsQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(MatchAllQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(QueryStringQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(BoostingQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(BoolQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(TermQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(TermsQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(FuzzyQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(RegexpQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(RangeQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(PrefixQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(WildcardQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(FilteredQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(ConstantScoreQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(SpanTermQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(SpanNotQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(FieldMaskingSpanQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(SpanFirstQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(SpanNearQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(SpanOrQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(MoreLikeThisQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(WrapperQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(IndicesQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(CommonTermsQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(SpanMultiTermQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(FunctionScoreQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(SimpleQueryStringParser.class).asEagerSingleton();
        qpBinders.addBinding().to(TemplateQueryParser.class).asEagerSingleton();

        if (ShapesAvailability.JTS_AVAILABLE) {
            qpBinders.addBinding().to(GeoShapeQueryParser.class).asEagerSingleton();
        }

        Multibinder<FilterParser> fpBinders = Multibinder.newSetBinder(binder(), FilterParser.class);
        for (Class<FilterParser> filterParser : filterParsersClasses) {
            fpBinders.addBinding().to(filterParser).asEagerSingleton();
        }
        for (FilterParser filterParser : filterParsers) {
            fpBinders.addBinding().toInstance(filterParser);
        }
        fpBinders.addBinding().to(HasChildFilterParser.class).asEagerSingleton();
        fpBinders.addBinding().to(HasParentFilterParser.class).asEagerSingleton();
        fpBinders.addBinding().to(NestedFilterParser.class).asEagerSingleton();
        fpBinders.addBinding().to(TypeFilterParser.class).asEagerSingleton();
        fpBinders.addBinding().to(IdsFilterParser.class).asEagerSingleton();
        fpBinders.addBinding().to(LimitFilterParser.class).asEagerSingleton();
        fpBinders.addBinding().to(TermFilterParser.class).asEagerSingleton();
        fpBinders.addBinding().to(TermsFilterParser.class).asEagerSingleton();
        fpBinders.addBinding().to(RangeFilterParser.class).asEagerSingleton();
        fpBinders.addBinding().to(PrefixFilterParser.class).asEagerSingleton();
        fpBinders.addBinding().to(RegexpFilterParser.class).asEagerSingleton();
        fpBinders.addBinding().to(ScriptFilterParser.class).asEagerSingleton();
        fpBinders.addBinding().to(GeoDistanceFilterParser.class).asEagerSingleton();
        fpBinders.addBinding().to(GeoDistanceRangeFilterParser.class).asEagerSingleton();
        fpBinders.addBinding().to(GeoBoundingBoxFilterParser.class).asEagerSingleton();
        fpBinders.addBinding().to(GeohashCellFilter.Parser.class).asEagerSingleton();
        fpBinders.addBinding().to(GeoPolygonFilterParser.class).asEagerSingleton();
        if (ShapesAvailability.JTS_AVAILABLE) {
            fpBinders.addBinding().to(GeoShapeFilterParser.class).asEagerSingleton();
        }
        fpBinders.addBinding().to(QueryFilterParser.class).asEagerSingleton();
        fpBinders.addBinding().to(FQueryFilterParser.class).asEagerSingleton();
        fpBinders.addBinding().to(BoolFilterParser.class).asEagerSingleton();
        fpBinders.addBinding().to(AndFilterParser.class).asEagerSingleton();
        fpBinders.addBinding().to(OrFilterParser.class).asEagerSingleton();
        fpBinders.addBinding().to(NotFilterParser.class).asEagerSingleton();
        fpBinders.addBinding().to(MatchAllFilterParser.class).asEagerSingleton();
        fpBinders.addBinding().to(ExistsFilterParser.class).asEagerSingleton();
        fpBinders.addBinding().to(MissingFilterParser.class).asEagerSingleton();
        fpBinders.addBinding().to(IndicesFilterParser.class).asEagerSingleton();
        fpBinders.addBinding().to(WrapperFilterParser.class).asEagerSingleton();
    }
}
