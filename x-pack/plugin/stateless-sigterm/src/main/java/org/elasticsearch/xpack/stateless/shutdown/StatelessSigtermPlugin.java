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

package org.elasticsearch.xpack.stateless.shutdown;

import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.node.internal.TerminationHandler;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xpack.shutdown.NodeSeenService;
import org.elasticsearch.xpack.shutdown.RestGetShutdownStatusAction;
import org.elasticsearch.xpack.shutdown.ShutdownPlugin;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Replaces the Node Shutdown plugin to make it work as desired for Serverless, which is primarily converting how k8s wants to talk to
 * Elasticsearch (SIGTERM, you're welcome) into Node Shutdown requests.
 */
public class StatelessSigtermPlugin extends ShutdownPlugin {
    private SigtermTerminationHandler terminationHandler;
    /**
     * The maximum amount of time to wait for an orderly shutdown.
     */
    public static final Setting<TimeValue> TIMEOUT_SETTING = Setting.timeSetting(
        "stateless.sigterm.timeout",
        new TimeValue(1, TimeUnit.HOURS),
        TimeValue.ZERO,
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> POLL_INTERVAL_SETTING = Setting.timeSetting(
        "stateless.sigterm.poll_interval",
        new TimeValue(1, TimeUnit.MINUTES),
        TimeValue.ZERO,
        Setting.Property.NodeScope
    );

    @Override
    public Collection<?> createComponents(PluginServices services) {
        ClusterService clusterService = services.clusterService();

        this.terminationHandler = new SigtermTerminationHandler(
            services.client(),
            services.threadPool(),
            services.clusterService(),
            services.remoteTransportClient(),
            POLL_INTERVAL_SETTING.get(clusterService.getSettings()),
            TIMEOUT_SETTING.get(clusterService.getSettings()),
            services.nodeEnvironment().nodeId()
        );

        NodeSeenService nodeSeenService = new NodeSeenService(clusterService);
        SigtermShutdownCleanupService shutdownCleanupService = new SigtermShutdownCleanupService(clusterService, services.rerouteService());
        clusterService.addListener(shutdownCleanupService);
        return List.of(nodeSeenService, shutdownCleanupService);
    }

    @Override
    public List<RestHandler> getRestHandlers(
        RestHandlersServices restHandlersServices,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        // Nothing should be calling the Put or Delete Shutdown APIs from the REST layer, so don't register them.
        // Exposing Get Status internally will be useful for troubleshooting.
        return Collections.singletonList(new RestGetShutdownStatusAction());
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(TIMEOUT_SETTING, POLL_INTERVAL_SETTING);
    }

    TerminationHandler getTerminationHandler() {
        return this.terminationHandler;
    }
}
