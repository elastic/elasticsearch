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

package org.elasticsearch.indices.recovery;

import org.apache.lucene.store.RateLimiter;
import org.apache.lucene.store.RateLimiter.SimpleRateLimiter;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

public class RecoverySettings extends AbstractComponent {

    public static final Setting<ByteSizeValue> INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING =
        Setting.byteSizeSetting("indices.recovery.max_bytes_per_sec", new ByteSizeValue(40, ByteSizeUnit.MB),
            Property.Dynamic, Property.NodeScope);

    /**
     * how long to wait before retrying after issues cause by cluster state syncing between nodes
     * i.e., local node is not yet known on remote node, remote shard not yet started etc.
     */
    public static final Setting<TimeValue> INDICES_RECOVERY_RETRY_DELAY_STATE_SYNC_SETTING =
        Setting.positiveTimeSetting("indices.recovery.retry_delay_state_sync", TimeValue.timeValueMillis(500),
            Property.Dynamic, Property.NodeScope);

    /** how long to wait before retrying after network related issues */
    public static final Setting<TimeValue> INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING =
        Setting.positiveTimeSetting("indices.recovery.retry_delay_network", TimeValue.timeValueSeconds(5),
            Property.Dynamic, Property.NodeScope);

    /** timeout value to use for requests made as part of the recovery process */
    public static final Setting<TimeValue> INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT_SETTING =
        Setting.positiveTimeSetting("indices.recovery.internal_action_timeout", TimeValue.timeValueMinutes(15),
            Property.Dynamic, Property.NodeScope);

    /**
     * timeout value to use for requests made as part of the recovery process that are expected to take long time.
     * defaults to twice `indices.recovery.internal_action_timeout`.
     */
    public static final Setting<TimeValue> INDICES_RECOVERY_INTERNAL_LONG_ACTION_TIMEOUT_SETTING =
        Setting.timeSetting("indices.recovery.internal_action_long_timeout",
            (s) -> TimeValue.timeValueMillis(INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT_SETTING.get(s).millis() * 2).toString(),
            TimeValue.timeValueSeconds(0), Property.Dynamic,  Property.NodeScope);

    /**
     * recoveries that don't show any activity for more then this interval will be failed.
     * defaults to `indices.recovery.internal_action_long_timeout`
     */
    public static final Setting<TimeValue> INDICES_RECOVERY_ACTIVITY_TIMEOUT_SETTING =
        Setting.timeSetting("indices.recovery.recovery_activity_timeout",
            (s) -> INDICES_RECOVERY_INTERNAL_LONG_ACTION_TIMEOUT_SETTING.getRaw(s) , TimeValue.timeValueSeconds(0),
            Property.Dynamic, Property.NodeScope);

    public static final ByteSizeValue DEFAULT_CHUNK_SIZE = new ByteSizeValue(512, ByteSizeUnit.KB);

    private volatile ByteSizeValue maxBytesPerSec;
    private volatile SimpleRateLimiter rateLimiter;
    private volatile TimeValue retryDelayStateSync;
    private volatile TimeValue retryDelayNetwork;
    private volatile TimeValue activityTimeout;
    private volatile TimeValue internalActionTimeout;
    private volatile TimeValue internalActionLongTimeout;

    private volatile ByteSizeValue chunkSize = DEFAULT_CHUNK_SIZE;

    @Inject
    public RecoverySettings(Settings settings, ClusterSettings clusterSettings) {
        super(settings);

        this.retryDelayStateSync = INDICES_RECOVERY_RETRY_DELAY_STATE_SYNC_SETTING.get(settings);
        // doesn't have to be fast as nodes are reconnected every 10s by default (see InternalClusterService.ReconnectToNodes)
        // and we want to give the master time to remove a faulty node
        this.retryDelayNetwork = INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING.get(settings);

        this.internalActionTimeout = INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT_SETTING.get(settings);
        this.internalActionLongTimeout = INDICES_RECOVERY_INTERNAL_LONG_ACTION_TIMEOUT_SETTING.get(settings);

        this.activityTimeout = INDICES_RECOVERY_ACTIVITY_TIMEOUT_SETTING.get(settings);
        this.maxBytesPerSec = INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.get(settings);
        if (maxBytesPerSec.bytes() <= 0) {
            rateLimiter = null;
        } else {
            rateLimiter = new SimpleRateLimiter(maxBytesPerSec.mbFrac());
        }


        logger.debug("using max_bytes_per_sec[{}]", maxBytesPerSec);

        clusterSettings.addSettingsUpdateConsumer(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING, this::setMaxBytesPerSec);
        clusterSettings.addSettingsUpdateConsumer(INDICES_RECOVERY_RETRY_DELAY_STATE_SYNC_SETTING, this::setRetryDelayStateSync);
        clusterSettings.addSettingsUpdateConsumer(INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING, this::setRetryDelayNetwork);
        clusterSettings.addSettingsUpdateConsumer(INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT_SETTING, this::setInternalActionTimeout);
        clusterSettings.addSettingsUpdateConsumer(INDICES_RECOVERY_INTERNAL_LONG_ACTION_TIMEOUT_SETTING, this::setInternalActionLongTimeout);
        clusterSettings.addSettingsUpdateConsumer(INDICES_RECOVERY_ACTIVITY_TIMEOUT_SETTING, this::setActivityTimeout);
    }

    public RateLimiter rateLimiter() {
        return rateLimiter;
    }

    public TimeValue retryDelayNetwork() {
        return retryDelayNetwork;
    }

    public TimeValue retryDelayStateSync() {
        return retryDelayStateSync;
    }

    public TimeValue activityTimeout() {
        return activityTimeout;
    }

    public TimeValue internalActionTimeout() {
        return internalActionTimeout;
    }

    public TimeValue internalActionLongTimeout() {
        return internalActionLongTimeout;
    }

    public ByteSizeValue getChunkSize() { return chunkSize; }

    void setChunkSize(ByteSizeValue chunkSize) { // only settable for tests
        if (chunkSize.bytesAsInt() <= 0) {
            throw new IllegalArgumentException("chunkSize must be > 0");
        }
        this.chunkSize = chunkSize;
    }


    public void setRetryDelayStateSync(TimeValue retryDelayStateSync) {
        this.retryDelayStateSync = retryDelayStateSync;
    }

    public void setRetryDelayNetwork(TimeValue retryDelayNetwork) {
        this.retryDelayNetwork = retryDelayNetwork;
    }

    public void setActivityTimeout(TimeValue activityTimeout) {
        this.activityTimeout = activityTimeout;
    }

    public void setInternalActionTimeout(TimeValue internalActionTimeout) {
        this.internalActionTimeout = internalActionTimeout;
    }

    public void setInternalActionLongTimeout(TimeValue internalActionLongTimeout) {
        this.internalActionLongTimeout = internalActionLongTimeout;
    }

    private void setMaxBytesPerSec(ByteSizeValue maxBytesPerSec) {
        this.maxBytesPerSec = maxBytesPerSec;
        if (maxBytesPerSec.bytes() <= 0) {
            rateLimiter = null;
        } else if (rateLimiter != null) {
            rateLimiter.setMBPerSec(maxBytesPerSec.mbFrac());
        } else {
            rateLimiter = new SimpleRateLimiter(maxBytesPerSec.mbFrac());
        }
    }
}
