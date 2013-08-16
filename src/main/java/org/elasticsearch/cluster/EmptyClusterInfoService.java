package org.elasticsearch.cluster;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.ImmutableSettings;

/**
 * ClusterInfoService that provides empty maps for disk usage and shard sizes
 */
public class EmptyClusterInfoService extends AbstractComponent implements ClusterInfoService {

    private final static class Holder {
        private final static EmptyClusterInfoService instance = new EmptyClusterInfoService();
    }
    private final ClusterInfo emptyClusterInfo;

    private EmptyClusterInfoService() {
        super(ImmutableSettings.EMPTY);
        emptyClusterInfo = new ClusterInfo(ImmutableMap.<String, DiskUsage>of(), ImmutableMap.<String, Long>of());
    }

    public static EmptyClusterInfoService getInstance() {
        return Holder.instance;
    }

    @Override
    public ClusterInfo getClusterInfo() {
        return emptyClusterInfo;
    }
}
