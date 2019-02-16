package org.elasticsearch.xpack.core.security.transport;

import org.elasticsearch.common.network.CloseableChannel;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import java.util.Set;

import static org.elasticsearch.common.settings.Setting.boolSetting;

public class DualStackCoordinator {

    public static final Setting<Boolean> DUAL_STACK_ENABLED =
        boolSetting("xpack.security.transport.ssl.dual_stack.enabled", false, Setting.Property.NodeScope, Setting.Property.Dynamic);

    private volatile Set<CloseableChannel> plaintextChannels = null;

    public DualStackCoordinator(ClusterSettings clusterSettings) {
        clusterSettings.addSettingsUpdateConsumer(DUAL_STACK_ENABLED, this::setDualStack);
    }

    public void registerPlaintextChannel(CloseableChannel channel) {
        Set<CloseableChannel> localPlaintextChannels = plaintextChannels;
        if (localPlaintextChannels != null) {
            localPlaintextChannels.add(channel);
            if (plaintextChannels == null) {
                CloseableChannel.closeChannel(channel);
            }
        } else {
            CloseableChannel.closeChannel(channel);
        }
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
