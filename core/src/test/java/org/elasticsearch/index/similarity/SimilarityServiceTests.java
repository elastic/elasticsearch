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
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;

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
        Settings settings = Settings.builder().put("index.similarity.default.type", "classic")
                .build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", settings);
        SimilarityService service = new SimilarityService(indexSettings, null, Collections.emptyMap());
        assertTrue(service.getDefaultSimilarity() instanceof ClassicSimilarity);
    }
}
