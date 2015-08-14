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

package org.elasticsearch.node;

import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.node.settings.NodeSettingsService;

/**
 *
 */
public class NodeModule extends AbstractModule {

    private final Node node;

    // pkg private so tests can mock
    Class<? extends PageCacheRecycler> pageCacheRecyclerImpl = PageCacheRecycler.class;
    Class<? extends BigArrays> bigArraysImpl = BigArrays.class;

    public NodeModule(Node node) {
        this.node = node;
    }

    @Override
    protected void configure() {
        if (pageCacheRecyclerImpl == PageCacheRecycler.class) {
            bind(PageCacheRecycler.class).asEagerSingleton();
        } else {
            bind(PageCacheRecycler.class).to(pageCacheRecyclerImpl).asEagerSingleton();
        }
        if (bigArraysImpl == BigArrays.class) {
            bind(BigArrays.class).asEagerSingleton();
        } else {
            bind(BigArrays.class).to(bigArraysImpl).asEagerSingleton();
        }

        bind(Node.class).toInstance(node);
        bind(NodeSettingsService.class).asEagerSingleton();
        bind(NodeService.class).asEagerSingleton();
    }
}
