/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.template.YamlTemplateRegistry;

public class OtelIndexTemplateRegistry extends YamlTemplateRegistry {

    public static final String OTEL_TEMPLATE_VERSION_VARIABLE = "xpack.oteldata.template.version";

    public OtelIndexTemplateRegistry(Settings nodeSettings, ClusterService clusterService, ThreadPool threadPool, Client client,
                                     NamedXContentRegistry xContentRegistry, FeatureService featureService) {
        super(nodeSettings, clusterService, threadPool, client, xContentRegistry, featureService);
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
