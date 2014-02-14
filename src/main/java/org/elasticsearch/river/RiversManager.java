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

package org.elasticsearch.river;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.river.cluster.RiverClusterService;
import org.elasticsearch.river.routing.RiversRouter;

/**
 *
 */
public class RiversManager extends AbstractLifecycleComponent<RiversManager> {

    private final RiversService riversService;

    private final RiverClusterService clusterService;

    private final RiversRouter riversRouter;

    @Inject
    public RiversManager(Settings settings, RiversService riversService, RiverClusterService clusterService, RiversRouter riversRouter) {
        super(settings);
        this.riversService = riversService;
        this.clusterService = clusterService;
        this.riversRouter = riversRouter;
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        riversRouter.start();
        riversService.start();
        clusterService.start();
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        riversRouter.stop();
        clusterService.stop();
        riversService.stop();
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        riversRouter.close();
        clusterService.close();
        riversService.close();
    }
}
