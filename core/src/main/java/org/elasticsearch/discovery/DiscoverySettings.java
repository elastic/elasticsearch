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

import java.util.EnumSet;

/**
 * Exposes common discovery settings that may be supported by all the different discovery implementations
 */
public class DiscoverySettings extends AbstractComponent {

    /**
     * sets the timeout for a complete publishing cycle, including both sending and committing. the master
     * will continute to process the next cluster state update after this time has elapsed
     **/
    public static final String PUBLISH_TIMEOUT = "discovery.zen.publish_timeout";

    /**
     * sets the timeout for receiving enough acks for a specific cluster state and committing it. failing
     * to receive responses within this window will cause the cluster state change to be rejected.
     */
    public static final String COMMIT_TIMEOUT = "discovery.zen.commit_timeout";
    public static final String NO_MASTER_BLOCK = "discovery.zen.no_master_block";
    public static final String PUBLISH_DIFF_ENABLE = "discovery.zen.publish_diff.enable";

    public static final TimeValue DEFAULT_PUBLISH_TIMEOUT = TimeValue.timeValueSeconds(30);
    public static final TimeValue DEFAULT_COMMIT_TIMEOUT = TimeValue.timeValueSeconds(30);
    public static final String DEFAULT_NO_MASTER_BLOCK = "write";
    public final static int NO_MASTER_BLOCK_ID = 2;
    public final static boolean DEFAULT_PUBLISH_DIFF_ENABLE = true;

    public final static ClusterBlock NO_MASTER_BLOCK_ALL = new ClusterBlock(NO_MASTER_BLOCK_ID, "no master", true, true, RestStatus.SERVICE_UNAVAILABLE, ClusterBlockLevel.ALL);
    public final static ClusterBlock NO_MASTER_BLOCK_WRITES = new ClusterBlock(NO_MASTER_BLOCK_ID, "no master", true, false, RestStatus.SERVICE_UNAVAILABLE, EnumSet.of(ClusterBlockLevel.WRITE, ClusterBlockLevel.METADATA_WRITE));

    private volatile ClusterBlock noMasterBlock;
    private volatile TimeValue publishTimeout;
    private volatile TimeValue commitTimeout;
    private volatile boolean publishDiff;

    @Inject
    public DiscoverySettings(Settings settings, NodeSettingsService nodeSettingsService) {
        super(settings);
        nodeSettingsService.addListener(new ApplySettings());
        this.noMasterBlock = parseNoMasterBlock(settings.get(NO_MASTER_BLOCK, DEFAULT_NO_MASTER_BLOCK));
        this.publishTimeout = settings.getAsTime(PUBLISH_TIMEOUT, DEFAULT_PUBLISH_TIMEOUT);
        this.commitTimeout = settings.getAsTime(COMMIT_TIMEOUT, new TimeValue(Math.min(DEFAULT_COMMIT_TIMEOUT.millis(), publishTimeout.millis())));
        this.publishDiff = settings.getAsBoolean(PUBLISH_DIFF_ENABLE, DEFAULT_PUBLISH_DIFF_ENABLE);
    }

    /**
     * Returns the current publish timeout
     */
    public TimeValue getPublishTimeout() {
        return publishTimeout;
    }

    public TimeValue getCommitTimeout() {
        return commitTimeout;
    }

    public ClusterBlock getNoMasterBlock() {
        return noMasterBlock;
    }

    public boolean getPublishDiff() { return publishDiff;}

    private class ApplySettings implements NodeSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            TimeValue newPublishTimeout = settings.getAsTime(PUBLISH_TIMEOUT, null);
            if (newPublishTimeout != null) {
                if (newPublishTimeout.millis() != publishTimeout.millis()) {
                    logger.info("updating [{}] from [{}] to [{}]", PUBLISH_TIMEOUT, publishTimeout, newPublishTimeout);
                    publishTimeout = newPublishTimeout;
                    if (settings.getAsTime(COMMIT_TIMEOUT, null) == null && commitTimeout.millis() > publishTimeout.millis()) {
                        logger.info("reducing default [{}] to [{}] due to publish timeout change", COMMIT_TIMEOUT, publishTimeout);
                        commitTimeout = publishTimeout;
                    }
                }
            }
            TimeValue newCommitTimeout = settings.getAsTime(COMMIT_TIMEOUT, null);
            if (newCommitTimeout != null) {
                if (newCommitTimeout.millis() != commitTimeout.millis()) {
                    logger.info("updating [{}] from [{}] to [{}]", COMMIT_TIMEOUT, commitTimeout, newCommitTimeout);
                    commitTimeout = newCommitTimeout;
                }
            }
            String newNoMasterBlockValue = settings.get(NO_MASTER_BLOCK);
            if (newNoMasterBlockValue != null) {
                ClusterBlock newNoMasterBlock = parseNoMasterBlock(newNoMasterBlockValue);
                if (newNoMasterBlock != noMasterBlock) {
                    noMasterBlock = newNoMasterBlock;
                }
            }
            Boolean newPublishDiff = settings.getAsBoolean(PUBLISH_DIFF_ENABLE, null);
            if (newPublishDiff != null) {
                if (newPublishDiff != publishDiff) {
                    logger.info("updating [{}] from [{}] to [{}]", PUBLISH_DIFF_ENABLE, publishDiff, newPublishDiff);
                    publishDiff = newPublishDiff;
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
                throw new IllegalArgumentException("invalid master block [" + value + "]");
        }
    }
}
