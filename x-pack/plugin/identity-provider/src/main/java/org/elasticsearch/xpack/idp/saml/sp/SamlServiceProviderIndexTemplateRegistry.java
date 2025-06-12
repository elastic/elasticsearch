/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.idp.saml.sp;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;
import org.elasticsearch.xpack.core.template.IndexTemplateRegistry;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderIndex.CURRENT_TEMPLATE_VERSION;
import static org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderIndex.FINAL_TEMPLATE_VERSION_STRING_DEPRECATED;
import static org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderIndex.TEMPLATE_NAME;
import static org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderIndex.TEMPLATE_RESOURCE;
import static org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderIndex.TEMPLATE_VERSION_STRING_DEPRECATED;
import static org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderIndex.TEMPLATE_VERSION_VARIABLE;

public class SamlServiceProviderIndexTemplateRegistry extends IndexTemplateRegistry {
    public SamlServiceProviderIndexTemplateRegistry(
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
    protected String getOrigin() {
        return "idp";
    }

    @Override
    protected List<IndexTemplateConfig> getLegacyTemplateConfigs() {
        return List.of(
            new IndexTemplateConfig(
                TEMPLATE_NAME,
                TEMPLATE_RESOURCE,
                CURRENT_TEMPLATE_VERSION,
                TEMPLATE_VERSION_VARIABLE,
                Map.of(TEMPLATE_VERSION_STRING_DEPRECATED, FINAL_TEMPLATE_VERSION_STRING_DEPRECATED)
            )
        );
    }
}
