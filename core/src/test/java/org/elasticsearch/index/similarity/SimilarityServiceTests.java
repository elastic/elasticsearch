/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.index.similarity;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;

import java.util.Collections;

public class SimilarityServiceTests extends ESTestCase {

    // Tests #16594
    public void testOverrideBuiltInSimilarity() {
        Settings settings = Settings.builder().put("index.similarity.BM25.type", "classic").build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", settings);
        try {
            new SimilarityService(indexSettings, Collections.emptyMap());
            fail("can't override bm25");
        } catch (IllegalArgumentException ex) {
            assertEquals(ex.getMessage(), "Cannot redefine built-in Similarity [BM25]");
        }
    }

    // Pre v3 indices could override built-in similarities
    public void testOverrideBuiltInSimilarityPreV3() {
        Settings settings = Settings.builder()
                                    .put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_2_0_0)
                                    .put("index.similarity.BM25.type", "classic")
                                    .build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", settings);
        SimilarityService service = new SimilarityService(indexSettings, Collections.emptyMap());
        assertTrue(service.getSimilarity("BM25") instanceof ClassicSimilarityProvider);
    }

    // Tests #16594
    public void testDefaultSimilarity() {
        Settings settings = Settings.builder().put("index.similarity.default.type", "BM25").build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", settings);
        SimilarityService service = new SimilarityService(indexSettings, Collections.emptyMap());
        assertTrue(service.getDefaultSimilarity() instanceof BM25SimilarityProvider);
    }
}
