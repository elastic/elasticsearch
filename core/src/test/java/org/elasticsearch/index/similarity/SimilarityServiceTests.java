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

import org.apache.lucene.search.similarities.BM25Similarity;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;

import java.util.Collections;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SimilarityServiceTests extends ESTestCase {
    public void testDefaultSimilarity() {
        Settings settings = Settings.builder().build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", settings);
        SimilarityService service = new SimilarityService(indexSettings, Collections.emptyMap());
        assertThat(service.getSimilarity("default"), instanceOf(BM25SimilarityProvider.class));
    }

    // Tests #16594
    public void testOverrideBuiltInSimilarity() {
        Settings settings = Settings.builder().put("index.similarity.BM25.type", "classic").build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", settings);
        IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () ->
            new SimilarityService(indexSettings, Collections.emptyMap()));
        assertThat(exc.getMessage(), containsString("Cannot redefine built-in Similarity [BM25]"));
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
    public void testOverrideDefaultSimilarity() {
        Settings settings = Settings.builder().put("index.similarity.default.type", "classic").build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", settings);
        SimilarityService service = new SimilarityService(indexSettings, Collections.emptyMap());
        assertTrue(service.getSimilarity("default") instanceof ClassicSimilarityProvider);
    }

    public void testInvalidSimilarityUpdates() {
        Settings settings = Settings.builder().put("index.similarity.custom.type", "BM25").build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", settings);
        SimilarityService service = new SimilarityService(indexSettings, Collections.emptyMap());
        assertThat(service.getSimilarity("custom"), instanceOf(BM25SimilarityProvider.class));

        {
            final Settings update = SimilarityService.SIMILARITY_SETTINGS.get(Settings.builder()
                .put("index.similarity.custom.type", "classic")
                .build());
            IllegalArgumentException exc =
                expectThrows(IllegalArgumentException.class, () -> service.validateSettings(update));
            assertThat(exc.getMessage(), containsString("setting [type] for similarity [custom], " +
                "not dynamically updateable"));
        }

        {
            final Settings update = SimilarityService.SIMILARITY_SETTINGS.get(Settings.builder()
                .put("index.similarity.custom.discount_overlaps", false)
                .build());
            IllegalArgumentException exc =
                expectThrows(IllegalArgumentException.class, () -> service.validateSettings(update));
            assertThat(exc.getMessage(), containsString("setting [discount_overlaps] for similarity [custom]," +
                " not dynamically updateable"));
        }
    }

    public void testSimilarityUpdates() {
        Settings settings = Settings.builder().put("index.similarity.custom.type", "BM25").build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", settings);
        SimilarityService service = new SimilarityService(indexSettings, Collections.emptyMap());
        assertThat(service.getSimilarity("custom"), instanceOf(BM25SimilarityProvider.class));

        settings = SimilarityService.SIMILARITY_SETTINGS.get(Settings.builder()
            .put("index.similarity.custom.type", "BM25")
            .put("index.similarity.custom.k1", 1.98f)
            .put("index.similarity.custom.b", 0.35f)
            .build());
        service.updateSettings(settings);
        assertThat(service.getSimilarity("custom"), instanceOf(BM25SimilarityProvider.class));
        BM25Similarity sim = (BM25Similarity) service.getSimilarity("custom").get();
        assertThat(sim.getK1(), equalTo(1.98f));
        assertThat(sim.getB(), equalTo(0.35f));
    }
}
