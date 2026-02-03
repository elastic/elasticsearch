/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.ccm;

import org.elasticsearch.test.ESTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CCMFeatureTests extends ESTestCase {

    public static CCMFeature createMockCCMFeature(boolean enabled) {
        var mockFeature = mock(CCMFeature.class);
        when(mockFeature.isCcmSupportedEnvironment()).thenReturn(enabled);

        return mockFeature;
    }

}
