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
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;

import java.util.EnumSet;

/**
 * Exposes common discovery settings that may be supported by all the different discovery implementations
 */
public class DiscoverySettings extends AbstractComponent {

    public static final int NO_MASTER_BLOCK_ID = 2;
    public static final ClusterBlock NO_MASTER_BLOCK_ALL = new ClusterBlock(NO_MASTER_BLOCK_ID, "no master", true, true, false, RestStatus.SERVICE_UNAVAILABLE, ClusterBlockLevel.ALL);
    public static final ClusterBlock NO_MASTER_BLOCK_WRITES = new ClusterBlock(NO_MASTER_BLOCK_ID, "no master", true, false, false, RestStatus.SERVICE_UNAVAILABLE, EnumSet.of(ClusterBlockLevel.WRITE, ClusterBlockLevel.METADATA_WRITE));
    /**
     * sets the timeout for a complete publishing cycle, including both sending and committing. the master
     * will continue to process the next cluster state update after this time has elapsed
     **/
    public static final Setting<TimeValue> PUBLISH_TIMEOUT_SETTING =
        Setting.positiveTimeSetting("discovery.zen.publish_timeout", TimeValue.timeValueSeconds(30),
            Property.Dynamic, Property.NodeScope);

    /**
     * sets the timeout for receiving enough acks for a specific cluster state and committing it. failing
     * to receive responses within this window will cause the cluster state change to be rejected.
     */
    public static final Setting<TimeValue> COMMIT_TIMEOUT_SETTING =
        new Setting<>("discovery.zen.commit_timeout", (s) -> PUBLISH_TIMEOUT_SETTING.getRaw(s),
            (s) -> TimeValue.parseTimeValue(s, TimeValue.timeValueSeconds(30), "discovery.zen.commit_timeout"),
            Property.Dynamic, Property.NodeScope);
    public static final Setting<ClusterBlock> NO_MASTER_BLOCK_SETTING =
        new Setting<>("discovery.zen.no_master_block", "write", DiscoverySettings::parseNoMasterBlock,
            Property.Dynamic, Property.NodeScope);
    public static final Setting<Boolean> PUBLISH_DIFF_ENABLE_SETTING =
        Setting.boolSetting("discovery.zen.publish_diff.enable", true, Property.Dynamic, Property.NodeScope);
    public static final Setting<TimeValue> INITIAL_STATE_TIMEOUT_SETTING =
        Setting.positiveTimeSetting("discovery.initial_state_timeout", TimeValue.timeValueSeconds(30), Property.NodeScope);

    private volatile ClusterBlock noMasterBlock;
    private volatile TimeValue publishTimeout;

    private volatile TimeValue commitTimeout;
    private volatile boolean publishDiff;

    public DiscoverySettings(Settings settings, ClusterSettings clusterSettings) {
        super(settings);
        clusterSettings.addSettingsUpdateConsumer(NO_MASTER_BLOCK_SETTING, this::setNoMasterBlock);
        clusterSettings.addSettingsUpdateConsumer(PUBLISH_DIFF_ENABLE_SETTING, this::setPublishDiff);
        clusterSettings.addSettingsUpdateConsumer(COMMIT_TIMEOUT_SETTING, this::setCommitTimeout);
        clusterSettings.addSettingsUpdateConsumer(PUBLISH_TIMEOUT_SETTING, this::setPublishTimeout);
        this.noMasterBlock = NO_MASTER_BLOCK_SETTING.get(settings);
        this.publishTimeout = PUBLISH_TIMEOUT_SETTING.get(settings);
        this.commitTimeout = COMMIT_TIMEOUT_SETTING.get(settings);
        this.publishDiff = PUBLISH_DIFF_ENABLE_SETTING.get(settings);
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

    private void setNoMasterBlock(ClusterBlock noMasterBlock) {
        this.noMasterBlock = noMasterBlock;
    }

    private void setPublishDiff(boolean publishDiff) {
        this.publishDiff = publishDiff;
    }

    private void setPublishTimeout(TimeValue publishTimeout) {
        this.publishTimeout = publishTimeout;
    }

    private void setCommitTimeout(TimeValue commitTimeout) {
        this.commitTimeout = commitTimeout;
    }

    public boolean getPublishDiff() { return publishDiff;}

    private static ClusterBlock parseNoMasterBlock(String value) {
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
