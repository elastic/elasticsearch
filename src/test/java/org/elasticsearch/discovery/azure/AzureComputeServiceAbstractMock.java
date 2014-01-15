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

package org.elasticsearch.discovery.azure;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cloud.azure.AzureComputeService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;

/**
 *
 */
public abstract class AzureComputeServiceAbstractMock extends AbstractLifecycleComponent<AzureComputeServiceAbstractMock>
    implements AzureComputeService {

    protected AzureComputeServiceAbstractMock(Settings settings) {
        super(settings);
        logger.debug("starting Azure Mock [{}]", this.getClass().getSimpleName());
    }

    @Override
    protected void doStart() throws ElasticSearchException {
        logger.debug("starting Azure Api Mock");
    }

    @Override
    protected void doStop() throws ElasticSearchException {
        logger.debug("stopping Azure Api Mock");
    }

    @Override
    protected void doClose() throws ElasticSearchException {
    }
}
