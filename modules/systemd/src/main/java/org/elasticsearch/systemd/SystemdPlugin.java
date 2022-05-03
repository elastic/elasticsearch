/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.systemd;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Build;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

public class SystemdPlugin extends Plugin implements ClusterPlugin {

    private static final Logger logger = LogManager.getLogger(SystemdPlugin.class);

    private final boolean enabled;

    final boolean isEnabled() {
        return enabled;
    }

    @SuppressWarnings("unused")
    public SystemdPlugin() {
        this(true, Build.CURRENT.type(), System.getenv("ES_SD_NOTIFY"));
    }

    SystemdPlugin(final boolean assertIsPackageDistribution, final Build.Type buildType, final String esSDNotify) {
        final boolean isPackageDistribution = buildType == Build.Type.DEB || buildType == Build.Type.RPM;
        if (assertIsPackageDistribution) {
            // our build is configured to only include this module in the package distributions
            assert isPackageDistribution : buildType;
        }
        if (isPackageDistribution == false) {
            logger.debug("disabling sd_notify as the build type [{}] is not a package distribution", buildType);
            enabled = false;
            return;
        }
        logger.trace("ES_SD_NOTIFY is set to [{}]", esSDNotify);
        if (esSDNotify == null) {
            enabled = false;
            return;
        }
        if (Boolean.TRUE.toString().equals(esSDNotify) == false && Boolean.FALSE.toString().equals(esSDNotify) == false) {
            throw new RuntimeException("ES_SD_NOTIFY set to unexpected value [" + esSDNotify + "]");
        }
        enabled = Boolean.TRUE.toString().equals(esSDNotify);
    }

    private final SetOnce<Scheduler.Cancellable> extender = new SetOnce<>();

    Scheduler.Cancellable extender() {
        return extender.get();
    }

    @Override
    public Collection<Object> createComponents(
        final Client client,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final ResourceWatcherService resourceWatcherService,
        final ScriptService scriptService,
        final NamedXContentRegistry xContentRegistry,
        final Environment environment,
        final NodeEnvironment nodeEnvironment,
        final NamedWriteableRegistry namedWriteableRegistry,
        final IndexNameExpressionResolver expressionResolver,
        final Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
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
        extender.set(threadPool.scheduleWithFixedDelay(() -> {
            final int rc = sd_notify(0, "EXTEND_TIMEOUT_USEC=30000000");
            if (rc < 0) {
                logger.warn("extending startup timeout via sd_notify failed with [{}]", rc);
            }
        }, TimeValue.timeValueSeconds(15), ThreadPool.Names.SAME));
        return List.of();
    }

    int sd_notify(@SuppressWarnings("SameParameterValue") final int unset_environment, final String state) {
        final int rc = Libsystemd.sd_notify(unset_environment, state);
        logger.trace("sd_notify({}, {}) returned [{}]", unset_environment, state, rc);
        return rc;
    }

    @Override
    public void onNodeStarted() {
        if (enabled == false) {
            assert extender.get() == null;
            return;
        }
        final int rc = sd_notify(0, "READY=1");
        if (rc < 0) {
            // treat failure to notify systemd of readiness as a startup failure
            throw new RuntimeException("sd_notify returned error [" + rc + "]");
        }
        assert extender.get() != null;
        final boolean cancelled = extender.get().cancel();
        assert cancelled;
    }

    @Override
    public void close() {
        if (enabled == false) {
            return;
        }
        final int rc = sd_notify(0, "STOPPING=1");
        if (rc < 0) {
            // do not treat failure to notify systemd of stopping as a failure
            logger.warn("sd_notify returned error [{}]", rc);
        }
    }

}
