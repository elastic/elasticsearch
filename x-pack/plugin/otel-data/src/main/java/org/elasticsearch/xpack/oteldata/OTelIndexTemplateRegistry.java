/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.template.YamlTemplateRegistry;

import static org.elasticsearch.xpack.oteldata.OTelPlugin.OTEL_DATA_REGISTRY_ENABLED;

public class OTelIndexTemplateRegistry extends YamlTemplateRegistry {

    public static final String OTEL_TEMPLATE_VERSION_VARIABLE = "xpack.oteldata.template.version";

    public OTelIndexTemplateRegistry(
        Settings nodeSettings,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        NamedXContentRegistry xContentRegistry,
        ProjectResolver projectResolver
    ) {
        super(nodeSettings, clusterService, threadPool, client, xContentRegistry, projectResolver);
    }

    @Override
    public void initialize() {
        super.initialize();
        if (isEnabled()) {
            clusterService.getClusterSettings().addSettingsUpdateConsumer(OTEL_DATA_REGISTRY_ENABLED, this::setEnabled);
        }
    }

    @Override
    protected String getOrigin() {
        return ClientHelper.OTEL_ORIGIN;
    }

    @Override
    public String getName() {
        return "OpenTelemetry";
    }

    @Override
    protected String getVersionProperty() {
        return OTEL_TEMPLATE_VERSION_VARIABLE;
    }
}
