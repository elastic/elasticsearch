/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.template.YamlTemplateRegistry;

import static org.elasticsearch.xpack.prometheus.PrometheusPlugin.PROMETHEUS_REGISTRY_ENABLED;

public class PrometheusIndexTemplateRegistry extends YamlTemplateRegistry {

    public static final String PROMETHEUS_TEMPLATE_VERSION_VARIABLE = "xpack.prometheus.template.version";

    public PrometheusIndexTemplateRegistry(
        Settings nodeSettings,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        NamedXContentRegistry xContentRegistry
    ) {
        super(nodeSettings, clusterService, threadPool, client, xContentRegistry);
    }

    @Override
    public void initialize() {
        super.initialize();
        if (isEnabled()) {
            clusterService.getClusterSettings().addSettingsUpdateConsumer(PROMETHEUS_REGISTRY_ENABLED, this::setEnabled);
        }
    }

    @Override
    protected String getOrigin() {
        return ClientHelper.PROMETHEUS_ORIGIN;
    }

    @Override
    public String getName() {
        return "Prometheus";
    }

    @Override
    protected String getVersionProperty() {
        return PROMETHEUS_TEMPLATE_VERSION_VARIABLE;
    }
}
