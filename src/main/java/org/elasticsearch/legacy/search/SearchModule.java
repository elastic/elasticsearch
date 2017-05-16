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

package org.elasticsearch.legacy.search;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.legacy.common.inject.AbstractModule;
import org.elasticsearch.legacy.common.inject.Module;
import org.elasticsearch.legacy.common.inject.SpawnModules;
import org.elasticsearch.legacy.index.query.functionscore.FunctionScoreModule;
import org.elasticsearch.legacy.index.search.morelikethis.MoreLikeThisFetchService;
import org.elasticsearch.legacy.search.action.SearchServiceTransportAction;
import org.elasticsearch.legacy.search.aggregations.AggregationModule;
import org.elasticsearch.legacy.search.controller.SearchPhaseController;
import org.elasticsearch.legacy.search.dfs.DfsPhase;
import org.elasticsearch.legacy.search.facet.FacetModule;
import org.elasticsearch.legacy.search.fetch.FetchPhase;
import org.elasticsearch.legacy.search.fetch.explain.ExplainFetchSubPhase;
import org.elasticsearch.legacy.search.fetch.fielddata.FieldDataFieldsFetchSubPhase;
import org.elasticsearch.legacy.search.fetch.matchedqueries.MatchedQueriesFetchSubPhase;
import org.elasticsearch.legacy.search.fetch.partial.PartialFieldsFetchSubPhase;
import org.elasticsearch.legacy.search.fetch.script.ScriptFieldsFetchSubPhase;
import org.elasticsearch.legacy.search.fetch.source.FetchSourceSubPhase;
import org.elasticsearch.legacy.search.fetch.version.VersionFetchSubPhase;
import org.elasticsearch.legacy.search.highlight.HighlightModule;
import org.elasticsearch.legacy.search.highlight.HighlightPhase;
import org.elasticsearch.legacy.search.query.QueryPhase;
import org.elasticsearch.legacy.search.suggest.SuggestModule;

/**
 *
 */
public class SearchModule extends AbstractModule implements SpawnModules {

    @Override
    public Iterable<? extends Module> spawnModules() {
        return ImmutableList.of(new TransportSearchModule(), new FacetModule(), new HighlightModule(), new SuggestModule(), new FunctionScoreModule(), new AggregationModule());
    }

    @Override
    protected void configure() {
        bind(DfsPhase.class).asEagerSingleton();
        bind(QueryPhase.class).asEagerSingleton();
        bind(SearchService.class).asEagerSingleton();
        bind(SearchPhaseController.class).asEagerSingleton();

        bind(FetchPhase.class).asEagerSingleton();
        bind(ExplainFetchSubPhase.class).asEagerSingleton();
        bind(FieldDataFieldsFetchSubPhase.class).asEagerSingleton();
        bind(ScriptFieldsFetchSubPhase.class).asEagerSingleton();
        bind(PartialFieldsFetchSubPhase.class).asEagerSingleton();
        bind(FetchSourceSubPhase.class).asEagerSingleton();
        bind(VersionFetchSubPhase.class).asEagerSingleton();
        bind(MatchedQueriesFetchSubPhase.class).asEagerSingleton();
        bind(HighlightPhase.class).asEagerSingleton();

        bind(SearchServiceTransportAction.class).asEagerSingleton();
        bind(MoreLikeThisFetchService.class).asEagerSingleton();
    }
}
