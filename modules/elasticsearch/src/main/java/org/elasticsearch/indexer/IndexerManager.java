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

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indexer.cluster.IndexerClusterService;
import org.elasticsearch.indexer.routing.IndexersRouter;

/**
 * @author kimchy (shay.banon)
 */
public class IndexerManager extends AbstractLifecycleComponent<IndexerManager> {

    private final IndexersService indexersService;

    private final IndexerClusterService clusterService;

    private final IndexersRouter indexersRouter;

    @Inject public IndexerManager(Settings settings, IndexersService indexersService, IndexerClusterService clusterService, IndexersRouter indexersRouter) {
        super(settings);
        this.indexersService = indexersService;
        this.clusterService = clusterService;
        this.indexersRouter = indexersRouter;
    }

    @Override protected void doStart() throws ElasticSearchException {
        indexersRouter.start();
        indexersService.start();
        clusterService.start();
    }

    @Override protected void doStop() throws ElasticSearchException {
        indexersRouter.stop();
        clusterService.stop();
        indexersService.stop();
    }

    @Override protected void doClose() throws ElasticSearchException {
        indexersRouter.close();
        clusterService.close();
        indexersService.close();
    }
}
