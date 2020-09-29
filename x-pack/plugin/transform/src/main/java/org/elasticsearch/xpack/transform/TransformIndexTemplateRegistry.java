/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform;

import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;
import org.elasticsearch.xpack.core.template.IndexTemplateRegistry;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

class TransformIndexTemplateRegistry extends IndexTemplateRegistry {

    private static final String ROOT_RESOURCE_PATH = "/org/elasticsearch/xpack/transform/";
    private static final String VERSION_PATTERN = "xpack.transform.version";
    private static final String VERSION_ID_PATTERN = VERSION_PATTERN + ".id";

    TransformIndexTemplateRegistry(Settings nodeSettings, ClusterService clusterService, ThreadPool threadPool,
                                          Client client, NamedXContentRegistry xContentRegistry) {
        super(nodeSettings, clusterService, threadPool, client, xContentRegistry);
    }

    @Override
    protected boolean requiresMasterNode() {
        return true;
    }

    @Override
    protected List<IndexTemplateConfig> getLegacyTemplateConfigs() {
        IndexTemplateConfig internalIndexTemplate =
            new IndexTemplateConfig(TransformInternalIndexConstants.LATEST_INDEX_VERSIONED_NAME,
                ROOT_RESOURCE_PATH + "internal_index_template.json", Version.CURRENT.id, VERSION_ID_PATTERN,
                Collections.singletonMap(VERSION_PATTERN, Version.CURRENT.toString()));

        IndexTemplateConfig notificationsIndexTemplate =
            new IndexTemplateConfig(TransformInternalIndexConstants.AUDIT_INDEX,
                ROOT_RESOURCE_PATH + "notifications_index_template.json", Version.CURRENT.id, VERSION_ID_PATTERN,
                Collections.singletonMap(VERSION_PATTERN, Version.CURRENT.toString()));

        return Arrays.asList(internalIndexTemplate, notificationsIndexTemplate);
    }

    @Override
    protected String getOrigin() {
        return ClientHelper.TRANSFORM_ORIGIN;
    }
}
