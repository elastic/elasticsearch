/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.async;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;
import org.elasticsearch.xpack.core.template.IndexTemplateRegistry;

import java.util.Collections;
import java.util.List;

/**
 * The {@link AsyncTaskTemplateRegistry} class sets up the index template for the .async-search index.
 */
public class AsyncTaskTemplateRegistry extends IndexTemplateRegistry {
    public static final int INDEX_TEMPLATE_VERSION = 0;

    public static final String ASYNC_TASK_TEMPLATE_VERSION_VARIABLE = "xpack.async_search.template.version";
    public static final String ASYNC_SEARCH_TEMPLATE_NAME = "async-search";

    public static final IndexTemplateConfig TEMPLATE_ASYNC_SEARCH = new IndexTemplateConfig(
        ASYNC_SEARCH_TEMPLATE_NAME,
        "/async-search.json",
        INDEX_TEMPLATE_VERSION,
        ASYNC_TASK_TEMPLATE_VERSION_VARIABLE
    );

    public AsyncTaskTemplateRegistry(Settings nodeSettings, ClusterService clusterService,
                                     ThreadPool threadPool, Client client,
                                     NamedXContentRegistry xContentRegistry) {
        super(nodeSettings, clusterService, threadPool, client, xContentRegistry);
    }

    @Override
    protected boolean requiresMasterNode() {
        return true;
    }

    @Override
    protected List<IndexTemplateConfig> getComposableTemplateConfigs() {
        return Collections.singletonList(TEMPLATE_ASYNC_SEARCH);
    }

    @Override
    protected String getOrigin() {
        return ClientHelper.ASYNC_SEARCH_ORIGIN;
    }

    public boolean validate(ClusterState state) {
        boolean allTemplatesPresent = getComposableTemplateConfigs().stream()
            .map(IndexTemplateConfig::getTemplateName)
            .allMatch(name -> state.metadata().templatesV2().containsKey(name));

        return allTemplatesPresent;
    }
}
