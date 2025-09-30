/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.inference.mock;

import org.elasticsearch.inference.DefaultConfigId;
import org.elasticsearch.plugins.InferencePlugin.TestInferenceServiceExtension;

import java.util.List;

import static org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService.DEFAULT_ELSER_ENDPOINT_ID_V2;

public class TestEisMockInferenceServiceExtension implements TestInferenceServiceExtension {

    @Override
    public List<DefaultConfigId> defaultConfigIds() {
        // This mocks the presence of the EIS ELSER v2 endpoint, making the test cluster believe it's available.
        return List.of(new DefaultConfigId("elser-2", DEFAULT_ELSER_ENDPOINT_ID_V2));
    }
}
