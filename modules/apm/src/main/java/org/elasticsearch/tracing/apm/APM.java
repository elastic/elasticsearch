/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tracing.apm;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.TracerPlugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

/**
 * This module integrates Elastic's APM product with Elasticsearch. Elasticsearch has
 * a {@link org.elasticsearch.tracing.Tracer} interface, which this module implements via
 * {@link APMTracer}. We use the OpenTelemetry API to capture "spans", and attach the
 * Elastic APM Java to ship those spans to an APM server. Although it is possible to
 * programmatically attach the agent, the Security Manager permissions required for this
 * make this approach difficult to the point of impossibility.
 * <p>
 * All settings are found under the <code>tracing.apm.</code> prefix. Any setting under
 * the <code>tracing.apm.agent.</code> prefix will be forwarded on to the APM Java agent
 * by setting appropriate system properties. Some settings can only be set once, and must be
 * set when the agent starts. We therefore also create and configure a config file in
 * the {@code APMJvmOptions} class, which we then delete when Elasticsearch starts, so that
 * sensitive settings such as <code>secret_token</code> or <code>api_key</code> are not
 * left on disk.
 * <p>
 * When settings are reconfigured using the settings REST API, the new values will again
 * be passed via system properties to the Java agent, which periodically checks for changes
 * and applies the new settings values, provided those settings can be dynamically updated.
 */
public class APM extends Plugin implements NetworkPlugin, TracerPlugin {
    private final SetOnce<APMTracer> tracer = new SetOnce<>();
    private final Settings settings;

    public APM(Settings settings) {
        this.settings = settings;
    }

    @Override
    public Tracer getTracer(Settings settings) {
        final APMTracer apmTracer = new APMTracer(settings);
        tracer.set(apmTracer);
        return apmTracer;
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier,
        Tracer unused,
        AllocationService allocationService
    ) {
        final APMTracer apmTracer = tracer.get();

        apmTracer.setClusterName(clusterService.getClusterName().value());
        apmTracer.setNodeName(clusterService.getNodeName());

        final APMAgentSettings apmAgentSettings = new APMAgentSettings();
        apmAgentSettings.syncAgentSystemProperties(settings);
        apmAgentSettings.addClusterSettingsListeners(clusterService, apmTracer);

        return List.of(apmTracer);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            APMAgentSettings.APM_ENABLED_SETTING,
            APMAgentSettings.APM_TRACING_NAMES_INCLUDE_SETTING,
            APMAgentSettings.APM_TRACING_NAMES_EXCLUDE_SETTING,
            APMAgentSettings.APM_TRACING_SANITIZE_FIELD_NAMES,
            APMAgentSettings.APM_AGENT_SETTINGS,
            APMAgentSettings.APM_SECRET_TOKEN_SETTING,
            APMAgentSettings.APM_API_KEY_SETTING
        );
    }
}
