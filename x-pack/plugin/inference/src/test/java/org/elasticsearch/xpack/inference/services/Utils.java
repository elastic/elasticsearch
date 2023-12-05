/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class Utils {
    public static Model getInvalidModel(String modelId, String serviceName) {
        var mockConfigs = mock(ModelConfigurations.class);
        when(mockConfigs.getModelId()).thenReturn(modelId);
        when(mockConfigs.getService()).thenReturn(serviceName);

        var mockModel = mock(Model.class);
        when(mockModel.getConfigurations()).thenReturn(mockConfigs);

        return mockModel;
    }
}
