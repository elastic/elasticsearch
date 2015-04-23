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

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.rest.RestStatus;

import java.util.EnumSet;

/**
 * Exposes common discovery settings that may be supported by all the different discovery implementations
 */
public class DiscoverySettings extends AbstractComponent {

    public static final String PUBLISH_TIMEOUT = "discovery.zen.publish_timeout";
    public static final String NO_MASTER_BLOCK = "discovery.zen.no_master_block";

    public static final TimeValue DEFAULT_PUBLISH_TIMEOUT = TimeValue.timeValueSeconds(30);
    public static final String DEFAULT_NO_MASTER_BLOCK = "write";
    public final static int NO_MASTER_BLOCK_ID = 2;

    public final static ClusterBlock NO_MASTER_BLOCK_ALL = new ClusterBlock(NO_MASTER_BLOCK_ID, "no master", true, true, RestStatus.SERVICE_UNAVAILABLE, ClusterBlockLevel.ALL);
    public final static ClusterBlock NO_MASTER_BLOCK_WRITES = new ClusterBlock(NO_MASTER_BLOCK_ID, "no master", true, false, RestStatus.SERVICE_UNAVAILABLE, EnumSet.of(ClusterBlockLevel.WRITE, ClusterBlockLevel.METADATA_WRITE));

    private volatile ClusterBlock noMasterBlock;
    private volatile TimeValue publishTimeout = DEFAULT_PUBLISH_TIMEOUT;

    @Inject
    public DiscoverySettings(Settings settings, NodeSettingsService nodeSettingsService) {
        super(settings);
        nodeSettingsService.addListener(new ApplySettings());
        this.noMasterBlock = parseNoMasterBlock(settings.get(NO_MASTER_BLOCK, DEFAULT_NO_MASTER_BLOCK));
        this.publishTimeout = settings.getAsTime(PUBLISH_TIMEOUT, publishTimeout);
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
            String newNoMasterBlockValue = settings.get(NO_MASTER_BLOCK);
            if (newNoMasterBlockValue != null) {
                ClusterBlock newNoMasterBlock = parseNoMasterBlock(newNoMasterBlockValue);
                if (newNoMasterBlock != noMasterBlock) {
                    noMasterBlock = newNoMasterBlock;
                }
            }
        }
    }

    private ClusterBlock parseNoMasterBlock(String value) {
        switch (value) {
            case "all":
                return NO_MASTER_BLOCK_ALL;
            case "write":
                return NO_MASTER_BLOCK_WRITES;
            default:
                throw new ElasticsearchIllegalArgumentException("invalid master block [" + value + "]");
        }
    }
}
