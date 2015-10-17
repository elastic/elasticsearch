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

package org.elasticsearch.index;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngineFactory;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.IndexSearcherWrapper;

/**
 *
 */
public class IndexModule extends AbstractModule {

    private final IndexMetaData indexMetaData;
    // pkg private so tests can mock
    Class<? extends EngineFactory> engineFactoryImpl = InternalEngineFactory.class;
    Class<? extends IndexSearcherWrapper> indexSearcherWrapper = null;

    public IndexModule(IndexMetaData indexMetaData) {
        this.indexMetaData = indexMetaData;
    }

    @Override
    protected void configure() {
        bind(EngineFactory.class).to(engineFactoryImpl).asEagerSingleton();
        if (indexSearcherWrapper == null) {
            bind(IndexSearcherWrapper.class).toProvider(Providers.of(null));
        } else {
            bind(IndexSearcherWrapper.class).to(indexSearcherWrapper).asEagerSingleton();
        }
        bind(IndexMetaData.class).toInstance(indexMetaData);
        bind(IndexService.class).asEagerSingleton();
        bind(IndexServicesProvider.class).asEagerSingleton();
        bind(MapperService.class).asEagerSingleton();
        bind(IndexFieldDataService.class).asEagerSingleton();
    }


}
