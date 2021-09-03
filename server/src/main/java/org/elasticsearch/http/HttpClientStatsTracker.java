/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Tracks a collection of {@link org.elasticsearch.http.HttpStats.ClientStats} for current and recently-closed HTTP connections.
 */
public class HttpClientStatsTracker {

    private static final Logger logger = LogManager.getLogger();

    private static final long PRUNE_THROTTLE_INTERVAL = TimeUnit.SECONDS.toMillis(60);
    private static final long MAX_CLIENT_STATS_AGE = TimeUnit.MINUTES.toMillis(5);

    private final Map<Integer, HttpStats.ClientStats> httpChannelStats = new ConcurrentHashMap<>();
    private final ThreadPool threadPool;

    private volatile long lastClientStatsPruneTime;
    private volatile boolean clientStatsEnabled;

    HttpClientStatsTracker(Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool) {
        this.threadPool = threadPool;
        clientStatsEnabled = HttpTransportSettings.SETTING_HTTP_CLIENT_STATS_ENABLED.get(settings);
        clusterSettings.addSettingsUpdateConsumer(HttpTransportSettings.SETTING_HTTP_CLIENT_STATS_ENABLED, this::enableClientStats);
    }

    /**
     * Prunes client stats of entries that have been disconnected for more than {@link #MAX_CLIENT_STATS_AGE} (i.e. 5 minutes).
     *
     * @param throttled When true, executes the prune process only if more than {@link #PRUNE_THROTTLE_INTERVAL} (i.e. 60 seconds) has
     *                  elapsed since the last execution.
     */
    private void pruneClientStats(boolean throttled) {
        if (clientStatsEnabled && throttled == false ||
            (threadPool.relativeTimeInMillis() - lastClientStatsPruneTime > PRUNE_THROTTLE_INTERVAL)) {
            long nowMillis = threadPool.absoluteTimeInMillis();
            for (var statsEntry : httpChannelStats.entrySet()) {
                long closedTimeMillis = statsEntry.getValue().closedTimeMillis;
                if (closedTimeMillis > 0 && (nowMillis - closedTimeMillis > MAX_CLIENT_STATS_AGE)) {
                    httpChannelStats.remove(statsEntry.getKey());
                }
            }
            lastClientStatsPruneTime = threadPool.relativeTimeInMillis();
        }
    }

    /**
     * Enables or disables collection of HTTP client stats.
     */
    private void enableClientStats(boolean enabled) {
        this.clientStatsEnabled = enabled;
        if (enabled == false) {
            // when disabling, immediately clear client stats
            httpChannelStats.clear();
        }
    }

    /**
     * Register the given channel with this tracker.
     *
     * @return the corresponding newly-created stats object, or {@code null} if disabled.
     */
    HttpStats.ClientStats addClientStats(final HttpChannel httpChannel) {
        if (clientStatsEnabled) {
            final HttpStats.ClientStats clientStats;
            if (httpChannel != null) {
                clientStats = new HttpStats.ClientStats(threadPool.absoluteTimeInMillis());
                httpChannelStats.put(getChannelKey(httpChannel), clientStats);
                httpChannel.addCloseListener(ActionListener.wrap(() -> {
                    try {
                        HttpStats.ClientStats disconnectedClientStats =
                            httpChannelStats.get(getChannelKey(httpChannel));
                        if (disconnectedClientStats != null) {
                            disconnectedClientStats.closedTimeMillis = threadPool.absoluteTimeInMillis();
                        }
                    } catch (Exception e) {
                        assert false : e; // the listener code above should never throw
                        logger.warn("error removing HTTP channel listener", e);
                    }
                }));
            } else {
                clientStats = null;
            }
            pruneClientStats(true);
            return clientStats;
        } else {
            return null;
        }
    }

    private static String getFirstValueForHeader(final HttpRequest request, final String header) {
        for (Map.Entry<String, List<String>> entry : request.getHeaders().entrySet()) {
            if (entry.getKey().equalsIgnoreCase(header)) {
                if (entry.getValue().size() > 0) {
                    return entry.getValue().get(0);
                }
            }
        }
        return null;
    }

    /**
     * Adjust the stats for the given channel to reflect the latest request received.
     */
    void updateClientStats(final HttpRequest httpRequest, final HttpChannel httpChannel) {
        if (clientStatsEnabled && httpChannel != null) {
            HttpStats.ClientStats clientStats = httpChannelStats.get(getChannelKey(httpChannel));
            if (clientStats == null) {
                // will always return a non-null value when httpChannel is non-null
                clientStats = addClientStats(httpChannel);
            }

            if (clientStats.agent == null) {
                final String elasticProductOrigin = getFirstValueForHeader(httpRequest, "x-elastic-product-origin");
                if (elasticProductOrigin != null) {
                    clientStats.agent = elasticProductOrigin;
                } else {
                    final String userAgent = getFirstValueForHeader(httpRequest, "User-Agent");
                    if (userAgent != null) {
                        clientStats.agent = userAgent;
                    }
                }
            }
            if (clientStats.localAddress == null) {
                clientStats.localAddress =
                    httpChannel.getLocalAddress() == null ? null : NetworkAddress.format(httpChannel.getLocalAddress());
                clientStats.remoteAddress =
                    httpChannel.getRemoteAddress() == null ? null : NetworkAddress.format(httpChannel.getRemoteAddress());
            }
            if (clientStats.forwardedFor == null) {
                final String forwardedFor = getFirstValueForHeader(httpRequest, "x-forwarded-for");
                if (forwardedFor != null) {
                    clientStats.forwardedFor = forwardedFor;
                }
            }
            if (clientStats.opaqueId == null) {
                final String opaqueId = getFirstValueForHeader(httpRequest, "x-opaque-id");
                if (opaqueId != null) {
                    clientStats.opaqueId = opaqueId;
                }
            }
            clientStats.lastRequestTimeMillis = threadPool.absoluteTimeInMillis();
            clientStats.lastUri = httpRequest.uri();
            clientStats.requestCount.increment();
            clientStats.requestSizeBytes.add(httpRequest.content().length());
        }
    }

    /**
     * @return a list of the stats for the channels that are currently being tracked.
     */
    List<HttpStats.ClientStats> getClientStats() {
        pruneClientStats(false);
        return new ArrayList<>(httpChannelStats.values());
    }

    /**
     * Returns a key suitable for use in a hash table for the specified HttpChannel
     */
    private static int getChannelKey(HttpChannel channel) {
        // always use an identity-based hash code rather than one based on object state
        return System.identityHashCode(channel);
    }

}
