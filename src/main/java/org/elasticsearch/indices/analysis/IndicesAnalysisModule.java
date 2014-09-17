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

package org.elasticsearch.indices.analysis;

import com.google.common.collect.Maps;
import org.apache.lucene.analysis.hunspell.Dictionary;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

import java.util.Map;

public class IndicesAnalysisModule extends AbstractModule {

    private final Map<String, Dictionary> hunspellDictionaries =  Maps.newHashMap();

    public void addHunspellDictionary(String lang, Dictionary dictionary) {
        hunspellDictionaries.put(lang, dictionary);
    }

    @Override
    protected void configure() {
        bind(IndicesAnalysisService.class).asEagerSingleton();

        MapBinder<String, Dictionary> dictionariesBinder = MapBinder.newMapBinder(binder(), String.class, Dictionary.class);
        for (Map.Entry<String, Dictionary> entry : hunspellDictionaries.entrySet()) {
            dictionariesBinder.addBinding(entry.getKey()).toInstance(entry.getValue());
        }
        bind(HunspellService.class).asEagerSingleton();
    }
}