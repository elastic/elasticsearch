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

package org.elasticsearch.search;

import com.google.inject.AbstractModule;
import org.elasticsearch.search.action.SearchServiceTransportAction;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.search.dfs.DfsPhase;
import org.elasticsearch.search.facets.FacetsPhase;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.query.QueryPhase;

/**
 * @author kimchy (Shay Banon)
 */
public class SearchModule extends AbstractModule {

    @Override protected void configure() {
        bind(DfsPhase.class).asEagerSingleton();
        bind(FacetsPhase.class).asEagerSingleton();
        bind(QueryPhase.class).asEagerSingleton();
        bind(FetchPhase.class).asEagerSingleton();
        bind(SearchService.class).asEagerSingleton();
        bind(SearchPhaseController.class).asEagerSingleton();

        bind(SearchServiceTransportAction.class).asEagerSingleton();
    }
}
