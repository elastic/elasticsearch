/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.services.sagemaker.model.SageMakerModelBuilder;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerSchemas;
import org.junit.Before;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.mockito.Mockito.mock;

public class SageMakerServiceTests extends ESTestCase {

    private SageMakerModelBuilder modelBuilder;
    private SageMakerClient client;
    private SageMakerSchemas schemas;
    private ThreadPool threadPool;

    @Before
    public void init() {
        modelBuilder = mock();
        client = mock();
        schemas = mock();
        threadPool = createThreadPool(inferenceUtilityPool());
    }

}
