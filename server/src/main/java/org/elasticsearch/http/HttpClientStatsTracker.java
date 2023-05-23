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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.threadpool.ThreadPool;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.function.LongPredicate;
import java.util.stream.Stream;

import static org.elasticsearch.http.HttpStats.ClientStats.NOT_CLOSED;

/**
 * Tracks a collection of {@link org.elasticsearch.http.HttpStats.ClientStats} for current and recently-closed HTTP connections.
 */
public class HttpClientStatsTracker {

    private static final Logger logger = LogManager.getLogger(HttpClientStatsTracker.class);

    private final ThreadPool threadPool;

    private final Map<HttpChannel, ClientStatsBuilder> httpChannelStats = new ConcurrentHashMap<>();
    private final Semaphore closedChannelPermits;
    private final ConcurrentLinkedQueue<HttpStats.ClientStats> closedChannelStats = new ConcurrentLinkedQueue<>();
    private final long maxClosedChannelAgeMillis;

    private volatile boolean clientStatsEnabled;

    HttpClientStatsTracker(Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool) {
        this.threadPool = threadPool;
        this.closedChannelPermits = new Semaphore(HttpTransportSettings.SETTING_HTTP_CLIENT_STATS_MAX_CLOSED_CHANNEL_COUNT.get(settings));
        this.maxClosedChannelAgeMillis = HttpTransportSettings.SETTING_HTTP_CLIENT_STATS_MAX_CLOSED_CHANNEL_AGE.get(settings).millis();
        this.clientStatsEnabled = HttpTransportSettings.SETTING_HTTP_CLIENT_STATS_ENABLED.get(settings);
        clusterSettings.addSettingsUpdateConsumer(HttpTransportSettings.SETTING_HTTP_CLIENT_STATS_ENABLED, this::enableClientStats);
    }

    /**
     * Enables or disables collection of HTTP client stats.
     */
    private void enableClientStats(boolean enabled) {
        this.clientStatsEnabled = enabled;

        if (enabled == false) {
            // stop tracking stats for open channels
            httpChannelStats.clear();

            // remove all stats for closed channels (NB best effort attempt, we might be concurrently adding some too, but they'll be pruned
            // the next time the stats are retrieved)
            pruneStaleClosedChannelStats(l -> false);
        }
    }

    /**
     * Register the given channel with this tracker.
     */
    void addClientStats(final HttpChannel httpChannel) {
        if (clientStatsEnabled == false) {
            return;
        }

        if (httpChannel == null) {
            return;
        }

        httpChannelStats.putIfAbsent(
            httpChannel,
            new ClientStatsBuilder(
                System.identityHashCode(httpChannel),
                formatAddress(httpChannel.getRemoteAddress()),
                threadPool.absoluteTimeInMillis()
            )
        );
        httpChannel.addCloseListener(ActionListener.running(() -> {
            try {
                final ClientStatsBuilder disconnectedClientStats = httpChannelStats.remove(httpChannel);
                if (disconnectedClientStats != null) {
                    addClosedChannelStats(disconnectedClientStats.build(threadPool.absoluteTimeInMillis()));
                }
            } catch (Exception e) {
                assert false : e; // the listener code above should never throw
                logger.warn("error removing HTTP channel listener", e);
            }
        }));
    }

    private void addClosedChannelStats(HttpStats.ClientStats clientStats) {
        if (clientStatsEnabled == false) {
            return;
        }

        if (closedChannelPermits.tryAcquire() == false) {
            // no room in the list, push out the oldest entry
            synchronized (closedChannelStats) {
                final HttpStats.ClientStats oldest = closedChannelStats.poll();
                if (oldest == null && closedChannelPermits.tryAcquire() == false) {
                    // The list is currently empty but no permits are available (and no prune is in progress). This can theoretically
                    // happen, e.g. if all the permits are held by other threads that haven't got around to adding their stats yet.
                    // Stupendously unlikely, and those other threads have fresher data anyway, so let's just give up.
                    return;
                }
            }
        }

        // we either acquired a permit or removed an item, so there's now room for our stats in the list
        closedChannelStats.add(clientStats);
    }

    /**
     * Adjust the stats for the given channel to reflect the latest request received.
     */
    void updateClientStats(final HttpRequest httpRequest, final HttpChannel httpChannel) {
        if (clientStatsEnabled && httpChannel != null) {
            final ClientStatsBuilder clientStats = httpChannelStats.get(httpChannel);
            if (clientStats != null) {
                clientStats.update(httpRequest, httpChannel, threadPool.absoluteTimeInMillis());
            }
        }
    }

    /**
     * @return a list of the stats for the channels that are currently being tracked.
     */
    List<HttpStats.ClientStats> getClientStats() {
        if (clientStatsEnabled) {
            final long currentTimeMillis = threadPool.absoluteTimeInMillis();
            final LongPredicate keepTimePredicate = closeTimeMillis -> currentTimeMillis - closeTimeMillis <= maxClosedChannelAgeMillis;
            pruneStaleClosedChannelStats(keepTimePredicate);
            return Stream.concat(
                closedChannelStats.stream().filter(c -> keepTimePredicate.test(c.closedTimeMillis())),
                httpChannelStats.values().stream().map(c -> c.build(NOT_CLOSED))
            ).toList();
        } else {
            // prune even if disabled since we don't prevent concurrently adding entries while being disabled
            httpChannelStats.clear();
            pruneStaleClosedChannelStats(l -> false);
            return Collections.emptyList();
        }
    }

    private void pruneStaleClosedChannelStats(LongPredicate keepTimePredicate) {
        synchronized (closedChannelStats) {
            while (true) {
                final HttpStats.ClientStats nextStats = closedChannelStats.peek();
                if (nextStats == null) {
                    return;
                }

                if (keepTimePredicate.test(nextStats.closedTimeMillis())) {
                    // the list elements are pretty much in the order in which the channels were closed so keep all the remaining items
                    return;
                }

                final HttpStats.ClientStats removed = closedChannelStats.poll();
                assert removed == nextStats; // synchronized (closedChannelStats) means nobody else did a poll() since the peek()
                closedChannelPermits.release();
            }
        }
    }

    @Nullable
    private static String formatAddress(@Nullable InetSocketAddress localAddress) {
        return localAddress == null ? null : NetworkAddress.format(localAddress);
    }

    private static class ClientStatsBuilder {
        final int id;
        final long openedTimeMillis;

        String agent;
        String localAddress;
        String remoteAddress;
        String lastUri;
        String forwardedFor;
        String opaqueId;
        long lastRequestTimeMillis = -1L;
        long requestCount;
        long requestSizeBytes;

        ClientStatsBuilder(int id, @Nullable String remoteAddress, long openedTimeMillis) {
            this.id = id;
            this.remoteAddress = remoteAddress;
            this.openedTimeMillis = openedTimeMillis;
        }

        synchronized void update(HttpRequest httpRequest, HttpChannel httpChannel, long currentTimeMillis) {
            if (agent == null) {
                final String elasticProductOrigin = getFirstValueForHeader(httpRequest, "x-elastic-product-origin");
                if (elasticProductOrigin != null) {
                    agent = elasticProductOrigin;
                } else {
                    agent = getFirstValueForHeader(httpRequest, "User-Agent");
                }
            }
            if (localAddress == null) {
                localAddress = formatAddress(httpChannel.getLocalAddress());
            }
            if (remoteAddress == null) {
                remoteAddress = formatAddress(httpChannel.getRemoteAddress());
            }
            if (forwardedFor == null) {
                forwardedFor = getFirstValueForHeader(httpRequest, "x-forwarded-for");
            }
            if (opaqueId == null) {
                opaqueId = getFirstValueForHeader(httpRequest, "x-opaque-id");
            }

            lastRequestTimeMillis = currentTimeMillis;
            lastUri = httpRequest.uri();
            requestCount += 1;
            requestSizeBytes += httpRequest.content().length();
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

        synchronized HttpStats.ClientStats build(long closedTimeMillis) {
            return new HttpStats.ClientStats(
                id,
                agent,
                localAddress,
                remoteAddress,
                lastUri,
                forwardedFor,
                opaqueId,
                openedTimeMillis,
                closedTimeMillis,
                lastRequestTimeMillis,
                requestCount,
                requestSizeBytes
            );
        }
    }

}
