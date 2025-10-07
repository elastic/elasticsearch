/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.cache;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;

/**
 * This class holds dynamic settings to control the behavior of {@link SearchCommitPrefetcher}.
 * <p>
 * It listens to changes in the cluster settings and updates its internal state accordingly.
 */
public class SearchCommitPrefetcherDynamicSettings {

    private static final TimeValue DEFAULT_SEARCH_IDLE_TIME = TimeValue.timeValueDays(3);
    public static final Setting<Boolean> PREFETCH_COMMITS_UPON_NOTIFICATIONS_ENABLED_SETTING = Setting.boolSetting(
        "stateless.search.prefetch_commits.enabled",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> PREFETCH_SEARCH_IDLE_TIME_SETTING = Setting.timeSetting(
        "stateless.search.prefetch_commits.search_idle_time",
        DEFAULT_SEARCH_IDLE_TIME,
        TimeValue.ZERO,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    private volatile boolean prefetchingEnabled;
    private volatile long prefetchSearchIdleTimeInMillis;

    public SearchCommitPrefetcherDynamicSettings(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(PREFETCH_COMMITS_UPON_NOTIFICATIONS_ENABLED_SETTING, value -> this.prefetchingEnabled = value);
        clusterSettings.initializeAndWatch(
            PREFETCH_SEARCH_IDLE_TIME_SETTING,
            value -> this.prefetchSearchIdleTimeInMillis = value.millis()
        );
    }

    public boolean prefetchingEnabled() {
        return prefetchingEnabled;
    }

    public long searchIdleTimeInMillis() {
        return prefetchSearchIdleTimeInMillis;
    }
}
