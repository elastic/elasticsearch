/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.rest;

import java.util.Set;

public interface TestFeatureService {
    boolean clusterHasFeature(String featureId);

    Set<String> getAllSupportedFeatures();

    TestFeatureService ALL_FEATURES = new TestFeatureService() {
        @Override
        public boolean clusterHasFeature(String featureId) {
            return true;
        }

        @Override
        public Set<String> getAllSupportedFeatures() {
            throw new UnsupportedOperationException("Only available to properly initialized TestFeatureService. See ESRestTestCase#createTestFeatureService");
        }
    };
}
