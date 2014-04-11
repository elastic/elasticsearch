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

package org.elasticsearch.discovery;

import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.rest.RestStatus;

/**
 * Exposes common discovery settings that may be supported by all the different discovery implementations
 */
public class DiscoverySettings extends AbstractComponent {

    public static final String PUBLISH_TIMEOUT = "discovery.zen.publish_timeout";
    public static final TimeValue DEFAULT_PUBLISH_TIMEOUT = TimeValue.timeValueSeconds(30);
    private volatile TimeValue publishTimeout = DEFAULT_PUBLISH_TIMEOUT;

    public final static int NO_MASTER_BLOCK_ID = 2;

    private final ClusterBlock noMasterBlock;

    @Inject
    public DiscoverySettings(Settings settings, NodeSettingsService nodeSettingsService) {
        super(settings);
        nodeSettingsService.addListener(new ApplySettings());
        this.noMasterBlock = new ClusterBlock(NO_MASTER_BLOCK_ID, "no master", true, true, RestStatus.SERVICE_UNAVAILABLE, ClusterBlockLevel.ALL);
    }

    /**
     * Returns the current publish timeout
     */
    public TimeValue getPublishTimeout() {
        return publishTimeout;
    }

    public ClusterBlock getNoMasterBlock() {
        return noMasterBlock;
    }

    private class ApplySettings implements NodeSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            TimeValue newPublishTimeout = settings.getAsTime(PUBLISH_TIMEOUT, null);
            if (newPublishTimeout != null) {
                if (newPublishTimeout.millis() != publishTimeout.millis()) {
                    logger.info("updating [{}] from [{}] to [{}]", PUBLISH_TIMEOUT, publishTimeout, newPublishTimeout);
                    publishTimeout = newPublishTimeout;
                }
            }
        }
    }
}
