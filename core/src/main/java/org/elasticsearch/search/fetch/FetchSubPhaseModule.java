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
package org.elasticsearch.search.fetch;

import com.google.common.collect.Lists;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.Multibinder;
import org.elasticsearch.search.fetch.explain.ExplainFetchSubPhase;
import org.elasticsearch.search.fetch.fielddata.FieldDataFieldsFetchSubPhase;
import org.elasticsearch.search.fetch.innerhits.InnerHitsFetchSubPhase;
import org.elasticsearch.search.fetch.matchedqueries.MatchedQueriesFetchSubPhase;
import org.elasticsearch.search.fetch.script.ScriptFieldsFetchSubPhase;
import org.elasticsearch.search.fetch.source.FetchSourceSubPhase;
import org.elasticsearch.search.fetch.version.VersionFetchSubPhase;
import org.elasticsearch.search.highlight.HighlightPhase;

import java.util.List;

/**
 *
 */
public class FetchSubPhaseModule extends AbstractModule {

    private List<Class<? extends FetchSubPhase>> fetchSubPhases = Lists.newArrayList();

    public FetchSubPhaseModule() {
        registerFetchSubPhase(ExplainFetchSubPhase.class);
        registerFetchSubPhase(FieldDataFieldsFetchSubPhase.class);
        registerFetchSubPhase(ScriptFieldsFetchSubPhase.class);
        registerFetchSubPhase(FetchSourceSubPhase.class);
        registerFetchSubPhase(VersionFetchSubPhase.class);
        registerFetchSubPhase(MatchedQueriesFetchSubPhase.class);
        registerFetchSubPhase(HighlightPhase.class);
    }

    public void registerFetchSubPhase(Class<? extends FetchSubPhase> subPhase) {
        fetchSubPhases.add(subPhase);
    }

    @Override
    protected void configure() {
        Multibinder<FetchSubPhase> parserMapBinder = Multibinder.newSetBinder(binder(), FetchSubPhase.class);
        for (Class<? extends FetchSubPhase> clazz : fetchSubPhases) {
            parserMapBinder.addBinding().to(clazz);
        }
        bind(InnerHitsFetchSubPhase.class).asEagerSingleton();
    }
}
