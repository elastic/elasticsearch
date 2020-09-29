/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.utils;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;
import org.elasticsearch.xpack.core.template.IndexTemplateRegistry;
import org.elasticsearch.xpack.core.template.LifecyclePolicyConfig;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;
import org.elasticsearch.xpack.transform.persistence.TransformInternalIndex;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

class TransformTemplateRegistry extends IndexTemplateRegistry {

    private static final String ROOT_RESOURCE_PATH = "/org/elasticsearch/xpack/core/ml/";
    private static final String ANOMALY_DETECTION_PATH = ROOT_RESOURCE_PATH + "anomalydetection/";
    private static final String VERSION_PATTERN = "xpack.transform.version";

    public TransformTemplateRegistry(Settings nodeSettings, ClusterService clusterService, ThreadPool threadPool,
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
            new IndexTemplateConfig(TransformInternalIndexConstants.LATEST_INDEX_VERSIONED_NAME)

/*
        try {
            templates.put(
                TransformInternalIndexConstants.LATEST_INDEX_VERSIONED_NAME,
                TransformInternalIndex.getIndexTemplateMetadata()
            );
        } catch (IOException e) {
            logger.error("Error creating transform index template", e);
        }
        try {
            templates.put(TransformInternalIndexConstants.AUDIT_INDEX, TransformInternalIndex.getAuditIndexTemplateMetadata());
        } catch (IOException e) {
            logger.warn("Error creating transform audit index", e);
        }
        */
    }

    @Override
    protected String getOrigin() {
        return ClientHelper.TRANSFORM_ORIGIN;
    }
}
