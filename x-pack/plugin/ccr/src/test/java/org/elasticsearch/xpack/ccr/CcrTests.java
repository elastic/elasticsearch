/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.ccr.index.engine.FollowingEngineFactory;

import java.io.IOException;
import java.util.Optional;

import static org.hamcrest.Matchers.instanceOf;

public class CcrTests extends ESTestCase {

    public void testGetEngineFactory() throws IOException {
        final Boolean[] values = new Boolean[] { true, false, null };
        for (final Boolean value : values) {
            final String indexName = "following-" + value;
            final Index index = new Index(indexName, UUIDs.randomBase64UUID());
            final Settings.Builder builder = Settings.builder()
                    .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetaData.SETTING_INDEX_UUID, index.getUUID());
            if (value != null) {
                builder.put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), value);
            }

            final IndexMetaData indexMetaData = new IndexMetaData.Builder(index.getName())
                    .settings(builder.build())
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .build();
            final Ccr ccr = new Ccr(Settings.EMPTY, new CcrLicenseChecker(() -> true, () -> false));
            final Optional<EngineFactory> engineFactory = ccr.getEngineFactory(new IndexSettings(indexMetaData, Settings.EMPTY));
            if (value != null && value) {
                assertTrue(engineFactory.isPresent());
                assertThat(engineFactory.get(), instanceOf(FollowingEngineFactory.class));
            } else {
                assertFalse(engineFactory.isPresent());
            }
        }
    }

}
