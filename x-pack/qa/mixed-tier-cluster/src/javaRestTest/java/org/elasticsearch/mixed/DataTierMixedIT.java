/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.mixed;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.rest.ESRestTestCase;

public class DataTierMixedIT extends ESRestTestCase {

    @Override
    protected boolean preserveSystemResources() {
        // bug in the ML reset API before v8.7
        // alternate approach: enable ML for older bwc versions
        return true;
    }

    public void testMixedTierCompatibility() throws Exception {
        createIndex(
            "test-index",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        ensureGreen("test-index");
    }
}
