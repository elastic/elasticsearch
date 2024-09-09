/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action.alibabacloudsearch;

import org.elasticsearch.inference.InputType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.embeddings.AlibabaCloudSearchEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.rerank.AlibabaCloudSearchRerankModel;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.sparse.AlibabaCloudSearchSparseModel;

import java.util.Map;

public interface AlibabaCloudSearchActionVisitor {
    ExecutableAction create(AlibabaCloudSearchEmbeddingsModel model, Map<String, Object> taskSettings, InputType inputType);

    ExecutableAction create(AlibabaCloudSearchSparseModel model, Map<String, Object> taskSettings, InputType inputType);

    ExecutableAction create(AlibabaCloudSearchRerankModel model, Map<String, Object> taskSettings);
}
