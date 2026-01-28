/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.apmdata;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.template.YamlTemplateRegistry;

import java.util.function.Predicate;

import static org.elasticsearch.cluster.metadata.DataStreamLifecycle.isDataStreamsLifecycleOnlyMode;
import static org.elasticsearch.xpack.apmdata.APMPlugin.APM_DATA_REGISTRY_ENABLED;

/**
 * Creates all index templates and ingest pipelines that are required for using Elastic APM.
 */
public class APMIndexTemplateRegistry extends YamlTemplateRegistry {

    public static final String APM_TEMPLATE_VERSION_VARIABLE = "xpack.apmdata.template.version";

    public APMIndexTemplateRegistry(
        Settings nodeSettings,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        NamedXContentRegistry xContentRegistry
    ) {
        super(
            nodeSettings,
            clusterService,
            threadPool,
            client,
            xContentRegistry,
            templateFilter(isDataStreamsLifecycleOnlyMode(clusterService.getSettings()))
        );
    }

    @Override
    public String getName() {
        return "apm";
    }

    @Override
    public void initialize() {
        super.initialize();
        if (isEnabled()) {
            clusterService.getClusterSettings().addSettingsUpdateConsumer(APM_DATA_REGISTRY_ENABLED, this::setEnabled);
        }
    }

    @Override
    protected String getVersionProperty() {
        return APM_TEMPLATE_VERSION_VARIABLE;
    }

    @Override
    protected String getOrigin() {
        return ClientHelper.APM_ORIGIN;
    }

    private static Predicate<String> templateFilter(boolean dslOnlyMode) {
        // Load ILM files only when the server supports ILM i.e. dsl-only-mode is false
        return templateName -> dslOnlyMode == false || templateName.endsWith("@ilm") == false;
    }
}
