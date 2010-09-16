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

package org.elasticsearch.indexer;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indexer.cluster.IndexerClusterService;
import org.elasticsearch.indexer.routing.IndexersRouter;

/**
 * @author kimchy (shay.banon)
 */
public class IndexersModule extends AbstractModule {

    private final Settings settings;

    public IndexersModule(Settings settings) {
        this.settings = settings;
    }

    @Override protected void configure() {
        bind(String.class).annotatedWith(IndexerIndexName.class).toInstance(settings.get("indexer.index_name", "indexer"));
        bind(IndexersService.class).asEagerSingleton();
        bind(IndexerClusterService.class).asEagerSingleton();
        bind(IndexersRouter.class).asEagerSingleton();
        bind(IndexerManager.class).asEagerSingleton();
    }
}
