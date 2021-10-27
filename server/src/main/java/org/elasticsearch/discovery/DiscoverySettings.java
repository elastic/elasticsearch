/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;

/**
 * Exposes common discovery settings that may be supported by all the different discovery implementations
 */
public class DiscoverySettings {

    /**
     * sets the timeout for a complete publishing cycle, including both sending and committing. the master
     * will continue to process the next cluster state update after this time has elapsed
     **/
    public static final Setting<TimeValue> PUBLISH_TIMEOUT_SETTING = Setting.positiveTimeSetting(
        "discovery.zen.publish_timeout",
        TimeValue.timeValueSeconds(30),
        Property.Dynamic,
        Property.NodeScope,
        Property.Deprecated
    );

    /**
     * sets the timeout for receiving enough acks for a specific cluster state and committing it. failing
     * to receive responses within this window will cause the cluster state change to be rejected.
     */
    public static final Setting<TimeValue> COMMIT_TIMEOUT_SETTING = new Setting<>(
        "discovery.zen.commit_timeout",
        PUBLISH_TIMEOUT_SETTING,
        (s) -> TimeValue.parseTimeValue(s, TimeValue.timeValueSeconds(30), "discovery.zen.commit_timeout"),
        Property.Deprecated,
        Property.Dynamic,
        Property.NodeScope
    );
    public static final Setting<Boolean> PUBLISH_DIFF_ENABLE_SETTING = Setting.boolSetting(
        "discovery.zen.publish_diff.enable",
        true,
        Property.Dynamic,
        Property.NodeScope,
        Property.Deprecated
    );
    public static final Setting<TimeValue> INITIAL_STATE_TIMEOUT_SETTING = Setting.positiveTimeSetting(
        "discovery.initial_state_timeout",
        TimeValue.timeValueSeconds(30),
        Property.NodeScope
    );

    private volatile TimeValue publishTimeout;

    private volatile TimeValue commitTimeout;
    private volatile boolean publishDiff;

    public DiscoverySettings(Settings settings, ClusterSettings clusterSettings) {
        clusterSettings.addSettingsUpdateConsumer(PUBLISH_DIFF_ENABLE_SETTING, this::setPublishDiff);
        clusterSettings.addSettingsUpdateConsumer(COMMIT_TIMEOUT_SETTING, this::setCommitTimeout);
        clusterSettings.addSettingsUpdateConsumer(PUBLISH_TIMEOUT_SETTING, this::setPublishTimeout);
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

    private void setPublishDiff(boolean publishDiff) {
        this.publishDiff = publishDiff;
    }

    private void setPublishTimeout(TimeValue publishTimeout) {
        this.publishTimeout = publishTimeout;
    }

    private void setCommitTimeout(TimeValue commitTimeout) {
        this.commitTimeout = commitTimeout;
    }

    public boolean getPublishDiff() {
        return publishDiff;
    }

}
