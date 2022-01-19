/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rollup.v2;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESTestCase;

public abstract class RollupTestCase extends ESTestCase {
    protected IndexMetadata newIndexMetadata(Settings settings) {
        Settings build = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(settings)
            .build();
        return IndexMetadata.builder("test").settings(build).build();
    }

    protected IndexSettings newIndexSettings(Settings settings) {
        return new IndexSettings(newIndexMetadata(settings), Settings.EMPTY);
    }
}
