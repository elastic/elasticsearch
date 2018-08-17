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

import org.apache.lucene.search.similarities.AfterEffectB;
import org.apache.lucene.search.similarities.AfterEffectL;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.BasicModelG;
import org.apache.lucene.search.similarities.BasicModelIne;
import org.apache.lucene.search.similarities.BooleanSimilarity;
import org.apache.lucene.search.similarities.DFRSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.hamcrest.Matchers;

import java.util.Collections;

import static org.hamcrest.Matchers.instanceOf;

public class SimilarityServiceTests extends ESTestCase {
    public void testDefaultSimilarity() {
        Settings settings = Settings.builder().build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", settings);
        SimilarityService service = new SimilarityService(indexSettings, null, Collections.emptyMap());
        assertThat(service.getDefaultSimilarity(), instanceOf(BM25Similarity.class));
    }

    // Tests #16594
    public void testOverrideBuiltInSimilarity() {
        Settings settings = Settings.builder().put("index.similarity.BM25.type", "classic").build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", settings);
        try {
            new SimilarityService(indexSettings, null, Collections.emptyMap());
            fail("can't override bm25");
        } catch (IllegalArgumentException ex) {
            assertEquals(ex.getMessage(), "Cannot redefine built-in Similarity [BM25]");
        }
    }

    public void testOverrideDefaultSimilarity() {
        Settings settings = Settings.builder().put("index.similarity.default.type", "boolean")
                .build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", settings);
        SimilarityService service = new SimilarityService(indexSettings, null, Collections.emptyMap());
        assertTrue(service.getDefaultSimilarity() instanceof BooleanSimilarity);
    }

    public void testDeprecatedDFRSimilarities() {
        Settings settings = Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_6_4_0)

                .put("index.similarity.my_sim1.type", "dfr")
                .put("index.similarity.my_sim1.model", "d")
                .put("index.similarity.my_sim1.normalization", "h2")
                .put("index.similarity.my_sim1.after_effect", "no")

                .put("index.similarity.my_sim2.type", "dfr")
                .put("index.similarity.my_sim2.model", "p")
                .put("index.similarity.my_sim2.normalization", "h2")
                .put("index.similarity.my_sim2.after_effect", "l")

                .put("index.similarity.my_sim2.type", "dfr")
                .put("index.similarity.my_sim2.model", "be")
                .put("index.similarity.my_sim2.normalization", "h2")
                .put("index.similarity.my_sim2.after_effect", "b")

                .build();

        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", settings);
        SimilarityService service = new SimilarityService(indexSettings, null, Collections.emptyMap());

        Similarity sim = service.getSimilarity("my_sim1").get();
        assertThat(sim, Matchers.instanceOf(DFRSimilarity.class));
        DFRSimilarity dfrSim = (DFRSimilarity) sim;
        assertThat(dfrSim.getBasicModel(), Matchers.instanceOf(BasicModelIne.class));
        assertThat(dfrSim.getAfterEffect(), Matchers.instanceOf(AfterEffectL.class));

        sim = service.getSimilarity("my_sim2").get();
        assertThat(sim, Matchers.instanceOf(DFRSimilarity.class));
        dfrSim = (DFRSimilarity) sim;
        assertThat(dfrSim.getBasicModel(), Matchers.instanceOf(BasicModelIne.class));
        assertThat(dfrSim.getAfterEffect(), Matchers.instanceOf(AfterEffectL.class));

        sim = service.getSimilarity("my_sim3").get();
        assertThat(sim, Matchers.instanceOf(DFRSimilarity.class));
        dfrSim = (DFRSimilarity) sim;
        assertThat(dfrSim.getBasicModel(), Matchers.instanceOf(BasicModelG.class));
        assertThat(dfrSim.getAfterEffect(), Matchers.instanceOf(AfterEffectB.class));

        assertWarnings(
                "Basic model [d] isn't supported anymore and has arbitrarily been replaced with [ine].",
                "Basic model [p] isn't supported anymore and has arbitrarily been replaced with [ine].",
                "Basic model [be] isn't supported anymore and has arbitrarily been replaced with [g].",
                "After effect [no] isn't supported anymore and has arbitrarily been replaced with [l].");
    }

    public void testRejectUnsupportedDFRSimilarities() {
        Settings settings = Settings.builder()
                .put("index.similarity.my_sim1.type", "dfr")
                .put("index.similarity.my_sim1.model", "d")
                .put("index.similarity.my_sim1.normalization", "h2")
                .put("index.similarity.my_sim1.after_effect", "l")
                .build();
        IndexSettings indexSettings1 = IndexSettingsModule.newIndexSettings("test", settings);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> new SimilarityService(indexSettings1, null, Collections.emptyMap()));
        assertEquals("Basic model [d] isn't supported anymore, please use another model.", e.getMessage());

        settings = Settings.builder()
                .put("index.similarity.my_sim1.type", "dfr")
                .put("index.similarity.my_sim1.model", "g")
                .put("index.similarity.my_sim1.normalization", "h2")
                .put("index.similarity.my_sim1.after_effect", "no")
                .build();
        IndexSettings indexSettings2 = IndexSettingsModule.newIndexSettings("test", settings);
        e = expectThrows(IllegalArgumentException.class,
                () -> new SimilarityService(indexSettings2, null, Collections.emptyMap()));
        assertEquals("After effect [no] isn't supported anymore, please use another effect.", e.getMessage());
    }
}
