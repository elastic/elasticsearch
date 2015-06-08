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

package org.elasticsearch.search;

import com.google.common.collect.ImmutableList;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.SpawnModules;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.functionscore.FunctionScoreModule;
import org.elasticsearch.index.search.morelikethis.MoreLikeThisFetchService;
import org.elasticsearch.search.action.SearchServiceTransportAction;
import org.elasticsearch.search.aggregations.AggregationModule;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.search.dfs.DfsPhase;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.explain.ExplainFetchSubPhase;
import org.elasticsearch.search.fetch.fielddata.FieldDataFieldsFetchSubPhase;
import org.elasticsearch.search.fetch.innerhits.InnerHitsFetchSubPhase;
import org.elasticsearch.search.fetch.matchedqueries.MatchedQueriesFetchSubPhase;
import org.elasticsearch.search.fetch.script.ScriptFieldsFetchSubPhase;
import org.elasticsearch.search.fetch.source.FetchSourceSubPhase;
import org.elasticsearch.search.fetch.version.VersionFetchSubPhase;
import org.elasticsearch.search.highlight.HighlightModule;
import org.elasticsearch.search.highlight.HighlightPhase;
import org.elasticsearch.search.query.QueryPhase;
import org.elasticsearch.search.suggest.SuggestModule;

/**
 *
 */
public class SearchModule extends AbstractModule implements SpawnModules {

    private final Settings settings;

    public SearchModule(Settings settings) {
        this.settings = settings;
    }

    @Override
    public Iterable<? extends Module> spawnModules() {
        return ImmutableList.of(
                new SearchServiceModule(settings),
                new TransportSearchModule(),
                new HighlightModule(),
                new SuggestModule(),
                new FunctionScoreModule(),
                new AggregationModule());
    }

    @Override
    protected void configure() {
        bind(DfsPhase.class).asEagerSingleton();
        bind(QueryPhase.class).asEagerSingleton();
        bind(SearchPhaseController.class).asEagerSingleton();

        bind(FetchPhase.class).asEagerSingleton();
        bind(ExplainFetchSubPhase.class).asEagerSingleton();
        bind(FieldDataFieldsFetchSubPhase.class).asEagerSingleton();
        bind(ScriptFieldsFetchSubPhase.class).asEagerSingleton();
        bind(FetchSourceSubPhase.class).asEagerSingleton();
        bind(VersionFetchSubPhase.class).asEagerSingleton();
        bind(MatchedQueriesFetchSubPhase.class).asEagerSingleton();
        bind(HighlightPhase.class).asEagerSingleton();
        bind(InnerHitsFetchSubPhase.class).asEagerSingleton();

        bind(SearchServiceTransportAction.class).asEagerSingleton();
        bind(MoreLikeThisFetchService.class).asEagerSingleton();
    }
}
