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

    private Set<Class<? extends QueryParser>> queryParsersClasses = Sets.newHashSet();

    public synchronized IndicesQueriesModule addQuery(Class<? extends QueryParser> queryParser) {
        queryParsersClasses.add(queryParser);
        return this;
    }

    @Override
    protected void configure() {
        bind(IndicesQueriesRegistry.class).asEagerSingleton();

        Multibinder<QueryParser> qpBinders = Multibinder.newSetBinder(binder(), QueryParser.class);
        for (Class<? extends QueryParser> queryParser : queryParsersClasses) {
            qpBinders.addBinding().to(queryParser).asEagerSingleton();
        }
        qpBinders.addBinding().to(MatchQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(MultiMatchQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(NestedQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(HasChildQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(HasParentQueryParser.class).asEagerSingleton();
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
        qpBinders.addBinding().to(SpanWithinQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(SpanContainingQueryParser.class).asEagerSingleton();
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
        qpBinders.addBinding().to(TypeQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(LimitQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(TermsQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(ScriptQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(GeoDistanceQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(GeoDistanceRangeQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(GeoBoundingBoxQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(GeohashCellQuery.Parser.class).asEagerSingleton();
        qpBinders.addBinding().to(GeoPolygonQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(QueryFilterParser.class).asEagerSingleton();
        qpBinders.addBinding().to(FQueryFilterParser.class).asEagerSingleton();
        qpBinders.addBinding().to(AndQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(OrQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(NotQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(ExistsQueryParser.class).asEagerSingleton();
        qpBinders.addBinding().to(MissingQueryParser.class).asEagerSingleton();

        if (ShapesAvailability.JTS_AVAILABLE) {
            qpBinders.addBinding().to(GeoShapeQueryParser.class).asEagerSingleton();
        }
    }
}
