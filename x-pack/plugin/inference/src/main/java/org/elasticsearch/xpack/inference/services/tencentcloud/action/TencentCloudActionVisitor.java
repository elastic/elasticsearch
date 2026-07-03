/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.action;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.tencentcloud.embeddings.TencentCloudEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.tencentcloud.rerank.TencentCloudRerankModel;

import java.util.Map;

public interface TencentCloudActionVisitor {

    ExecutableAction create(TencentCloudEmbeddingsModel model, Map<String, Object> taskSettings);

    ExecutableAction create(TencentCloudRerankModel model, Map<String, Object> taskSettings);
}
