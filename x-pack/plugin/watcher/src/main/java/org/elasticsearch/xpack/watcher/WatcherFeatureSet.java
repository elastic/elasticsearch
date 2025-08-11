/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.watcher.WatcherConstants;
import org.elasticsearch.xpack.core.watcher.WatcherFeatureSetUsage;
import org.elasticsearch.xpack.core.watcher.client.WatcherClient;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;
import org.elasticsearch.xpack.core.watcher.transport.actions.stats.WatcherStatsRequest;
import org.elasticsearch.xpack.core.watcher.transport.actions.stats.WatcherStatsResponse;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.WATCHER_ORIGIN;

public class WatcherFeatureSet implements XPackFeatureSet {

    private final boolean enabled;
    private final XPackLicenseState licenseState;
    private Client client;

    @Inject
    public WatcherFeatureSet(Settings settings, @Nullable XPackLicenseState licenseState, Client client) {
        this.enabled = XPackSettings.WATCHER_ENABLED.get(settings);
        this.licenseState = licenseState;
        this.client = client;
    }

    @Override
    public String name() {
        return XPackField.WATCHER;
    }

    @Override
    public boolean available() {
        return licenseState != null && WatcherConstants.WATCHER_FEATURE.checkWithoutTracking(licenseState);
    }

    @Override
    public boolean enabled() {
        return enabled;
    }

    @Override
    public Map<String, Object> nativeCodeInfo() {
        return null;
    }

    @Override
    public void usage(ActionListener<XPackFeatureSet.Usage> listener) {
        if (enabled) {
            ActionListener<XPackFeatureSet.Usage> preservingListener = ContextPreservingActionListener.wrapPreservingContext(
                listener,
                client.threadPool().getThreadContext()
            );
            try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(WATCHER_ORIGIN)) {
                WatcherClient watcherClient = new WatcherClient(client);
                WatcherStatsRequest request = new WatcherStatsRequest();
                request.includeStats(true);
                watcherClient.watcherStats(request, ActionListener.wrap(r -> {
                    List<Counters> countersPerNode = r.getNodes()
                        .stream()
                        .map(WatcherStatsResponse.Node::getStats)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
                    Counters mergedCounters = Counters.merge(countersPerNode);
                    preservingListener.onResponse(new WatcherFeatureSetUsage(available(), enabled(), mergedCounters.toNestedMap()));
                }, preservingListener::onFailure));
            }
        } else {
            listener.onResponse(new WatcherFeatureSetUsage(available(), enabled(), Collections.emptyMap()));
        }
    }
}
