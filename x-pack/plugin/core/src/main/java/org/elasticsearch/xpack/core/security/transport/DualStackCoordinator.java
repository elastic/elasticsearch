/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.transport;

import org.elasticsearch.common.network.CloseableChannel;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.xpack.core.XPackSettings;

import java.util.Set;

public class DualStackCoordinator {

    private volatile Set<CloseableChannel> plaintextChannels = null;

    public DualStackCoordinator(ClusterSettings clusterSettings) {
        setDualStack(clusterSettings.get(XPackSettings.DUAL_STACK_ENABLED));
        clusterSettings.addSettingsUpdateConsumer(XPackSettings.DUAL_STACK_ENABLED, this::setDualStack);
    }

    public void registerPlaintextChannel(CloseableChannel channel) {
        Set<CloseableChannel> preAddPlaintextChannels = plaintextChannels;
        if (preAddPlaintextChannels != null) {
            preAddPlaintextChannels.add(channel);
            Set<CloseableChannel> postAddPlaintextChannels = plaintextChannels;
            // If the plaintext channels Set identity has changed, it has been disabled since we added the
            // channel. We must close the channel now to ensure that a plaintext channel does not leak.
            if (postAddPlaintextChannels != preAddPlaintextChannels) {
                CloseableChannel.closeChannel(channel);
            }
        } else {
            CloseableChannel.closeChannel(channel);
        }
    }

    public boolean isDualStackEnabled() {
        return plaintextChannels != null;
    }

    private synchronized void setDualStack(boolean enabled) {
        if (enabled && plaintextChannels == null) {
            plaintextChannels = ConcurrentCollections.newConcurrentSet();
        } else if (enabled == false && plaintextChannels != null) {
            Set<CloseableChannel> localPlaintextChannels = plaintextChannels;
            plaintextChannels = null;
            for (CloseableChannel channel : localPlaintextChannels) {
                CloseableChannel.closeChannel(channel);
            }
        }
    }
}
