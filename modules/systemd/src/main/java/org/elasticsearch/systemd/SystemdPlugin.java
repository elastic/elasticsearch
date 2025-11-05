/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.systemd;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Build;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.nativeaccess.Systemd;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.Scheduler;

import java.util.Collection;
import java.util.List;

/**
 * Plugin that integrates Elasticsearch with systemd service management.
 * <p>
 * This plugin enables Elasticsearch to communicate with systemd on Linux systems when running
 * as a systemd service. It uses sd_notify to report service status including startup progress,
 * ready state, and shutdown notifications. The plugin is only active in package distributions
 * (DEB/RPM) when the ES_SD_NOTIFY environment variable is set to "true".
 * </p>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Plugin is automatically loaded by Elasticsearch in package distributions
 * // Enable systemd notifications by setting environment variable:
 * // ES_SD_NOTIFY=true
 *
 * // Plugin will automatically:
 * // - Extend systemd timeout during startup every 15 seconds
 * // - Notify systemd when Elasticsearch is ready
 * // - Notify systemd when Elasticsearch is stopping
 * }</pre>
 */
public class SystemdPlugin extends Plugin implements ClusterPlugin {

    private static final Logger logger = LogManager.getLogger(SystemdPlugin.class);

    private final boolean enabled;
    private final Systemd systemd;

    final boolean isEnabled() {
        return enabled;
    }

    /**
     * Constructs a new SystemdPlugin with default configuration.
     * <p>
     * This constructor checks the current build type and ES_SD_NOTIFY environment variable
     * to determine whether systemd integration should be enabled. It is automatically called
     * by the Elasticsearch plugin system.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Constructor is called automatically by Elasticsearch
     * // To enable systemd notifications, set:
     * // export ES_SD_NOTIFY=true
     * }</pre>
     */
    @SuppressWarnings("unused")
    public SystemdPlugin() {
        this(true, Build.current().type(), System.getenv("ES_SD_NOTIFY"));
    }

    SystemdPlugin(final boolean assertIsPackageDistribution, final Build.Type buildType, final String esSDNotify) {
        final boolean isPackageDistribution = buildType == Build.Type.DEB || buildType == Build.Type.RPM;
        if (assertIsPackageDistribution) {
            // our build is configured to only include this module in the package distributions
            assert isPackageDistribution : buildType;
        }
        if (isPackageDistribution == false) {
            logger.debug("disabling sd_notify as the build type [{}] is not a package distribution", buildType);
            this.enabled = false;
            this.systemd = null;
            return;
        }
        logger.trace("ES_SD_NOTIFY is set to [{}]", esSDNotify);
        if (esSDNotify == null) {
            this.enabled = false;
            this.systemd = null;
            return;
        }
        if (Boolean.TRUE.toString().equals(esSDNotify) == false && Boolean.FALSE.toString().equals(esSDNotify) == false) {
            throw new RuntimeException("ES_SD_NOTIFY set to unexpected value [" + esSDNotify + "]");
        }
        this.enabled = Boolean.TRUE.toString().equals(esSDNotify);
        this.systemd = enabled ? NativeAccess.instance().systemd() : null;
    }

    private final SetOnce<Scheduler.Cancellable> extender = new SetOnce<>();

    Scheduler.Cancellable extender() {
        return extender.get();
    }

    /**
     * Creates and initializes plugin components for systemd integration.
     * <p>
     * If systemd integration is enabled, this method schedules a recurring task that sends
     * timeout extension notifications to systemd every 15 seconds during startup. This prevents
     * systemd from timing out during long startup operations (e.g., metadata upgrades). The
     * scheduled task is cancelled once the node startup completes successfully.
     * </p>
     * <p>
     * Since systemd expects a READY=1 notification within 60 seconds by default, this method
     * ensures that systemd receives EXTEND_TIMEOUT_USEC notifications to extend the timeout
     * by 30 seconds every 15 seconds until startup completes.
     * </p>
     *
     * @param services the plugin services providing access to the thread pool
     * @return an empty collection (this plugin creates internal components only)
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Called automatically by Elasticsearch during plugin initialization
     * Collection<?> components = plugin.createComponents(pluginServices);
     * // If enabled, schedules periodic timeout extension notifications to systemd
     * }</pre>
     */
    @Override
    public Collection<?> createComponents(PluginServices services) {
        if (enabled == false) {
            extender.set(null);
            return List.of();
        }
        /*
         * Since we have set the service type to notify, by default systemd will wait up to sixty seconds for the process to send the
         * READY=1 status via sd_notify. Since our startup can take longer than that (e.g., if we are upgrading on-disk metadata) then we
         * need to repeatedly notify systemd that we are still starting up by sending EXTEND_TIMEOUT_USEC with an extension to the timeout.
         * Therefore, every fifteen seconds we send systemd a message via sd_notify to extend the timeout by thirty seconds. We will cancel
         * this scheduled task after we successfully notify systemd that we are ready.
         */
        extender.set(
            services.threadPool()
                .scheduleWithFixedDelay(
                    () -> { systemd.notify_extend_timeout(30); },
                    TimeValue.timeValueSeconds(15),
                    EsExecutors.DIRECT_EXECUTOR_SERVICE
                )
        );
        return List.of();
    }

    void notifyReady() {
        assert systemd != null;
        systemd.notify_ready();
    }

    void notifyStopping() {
        assert systemd != null;
        systemd.notify_stopping();
    }

    /**
     * Called when the Elasticsearch node has completed startup and is ready to serve requests.
     * <p>
     * This method sends a READY=1 notification to systemd via sd_notify, indicating that the
     * service has successfully started. It also cancels the recurring timeout extension task
     * that was scheduled during initialization, as it is no longer needed once the node is ready.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Called automatically by Elasticsearch after node startup completes
     * plugin.onNodeStarted();
     * // Sends READY=1 to systemd and cancels timeout extension task
     * }</pre>
     */
    @Override
    public void onNodeStarted() {
        if (enabled == false) {
            assert extender.get() == null;
            return;
        }
        notifyReady();
        assert extender.get() != null;
        final boolean cancelled = extender.get().cancel();
        assert cancelled;
    }

    /**
     * Called when the plugin is being closed during Elasticsearch shutdown.
     * <p>
     * This method sends a STOPPING=1 notification to systemd via sd_notify, indicating that
     * the service is shutting down gracefully. This allows systemd to track the service
     * lifecycle properly.
     * </p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Called automatically by Elasticsearch during shutdown
     * plugin.close();
     * // Sends STOPPING=1 to systemd
     * }</pre>
     */
    @Override
    public void close() {
        if (enabled == false) {
            return;
        }
        notifyStopping();
    }

}
