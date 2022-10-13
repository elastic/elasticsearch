/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.index;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteConnectionStrategy;

import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArraySet;

public class RemoteClusterResolver extends RemoteClusterAware {
    private final CopyOnWriteArraySet<String> clusters;

    public RemoteClusterResolver(Settings settings, ClusterSettings clusterSettings) {
        super(settings);
        clusters = new CopyOnWriteArraySet<>(getEnabledRemoteClusters(settings));
        listenForUpdates(clusterSettings);
    }

    @Override
    protected void updateRemoteCluster(String clusterAlias, Settings settings) {
        if (RemoteConnectionStrategy.isConnectionEnabled(clusterAlias, settings)) {
            clusters.add(clusterAlias);
        } else {
            clusters.remove(clusterAlias);
        }
    }

    public Set<String> remoteClusters() {
        return new TreeSet<>(clusters);
    }
}
