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

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.Similarity;
import org.elasticsearch.common.inject.ModuleTestCase;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;

import java.io.IOException;

public class SimilarityModuleTests extends ModuleTestCase {

    public void testAddSimilarity() {
        Settings indexSettings = Settings.settingsBuilder()
                .put("index.similarity.my_similarity.type", "test_similarity")
                .put("index.similarity.my_similarity.key", "there is a key")
                .build();
        SimilarityModule module = new SimilarityModule(new Index("foo"), indexSettings);
        module.addSimilarity("test_similarity", (string, settings) -> new SimilarityProvider() {
            @Override
            public String name() {
                return string;
            }

            @Override
            public Similarity get() {
                return new TestSimilarity(settings.get("key"));
            }
        });
        assertInstanceBinding(module, SimilarityService.class, (inst) -> {
            if (inst instanceof SimilarityService) {
                assertNotNull(inst.getSimilarity("my_similarity"));
                assertTrue(inst.getSimilarity("my_similarity").get() instanceof TestSimilarity);
                assertEquals("my_similarity", inst.getSimilarity("my_similarity").name());
                assertEquals("there is a key" , ((TestSimilarity)inst.getSimilarity("my_similarity").get()).key);
                return true;
            }
            return false;
        });
    }

    public void testSetupUnknownSimilarity() {
        Settings indexSettings = Settings.settingsBuilder()
                .put("index.similarity.my_similarity.type", "test_similarity")
                .build();
        SimilarityModule module = new SimilarityModule(new Index("foo"), indexSettings);
        try {
            assertInstanceBinding(module, SimilarityService.class, (inst) -> inst instanceof SimilarityService);
        } catch (IllegalArgumentException ex) {
            assertEquals("Unknown Similarity type [test_similarity] for [my_similarity]", ex.getMessage());
        }
    }


    public void testSetupWithoutType() {
        Settings indexSettings = Settings.settingsBuilder()
                .put("index.similarity.my_similarity.foo", "bar")
                .build();
        SimilarityModule module = new SimilarityModule(new Index("foo"), indexSettings);
        try {
            assertInstanceBinding(module, SimilarityService.class, (inst) -> inst instanceof SimilarityService);
        } catch (IllegalArgumentException ex) {
            assertEquals("Similarity [my_similarity] must have an associated type", ex.getMessage());
        }
    }


    private static class TestSimilarity extends Similarity {
        private final Similarity delegate = new BM25Similarity();
        private final String key;


        public TestSimilarity(String key) {
            if (key == null) {
                throw new AssertionError("key is null");
            }
            this.key = key;
        }

        @Override
        public long computeNorm(FieldInvertState state) {
            return delegate.computeNorm(state);
        }

        @Override
        public SimWeight computeWeight(CollectionStatistics collectionStats, TermStatistics... termStats) {
            return delegate.computeWeight(collectionStats, termStats);
        }

        @Override
        public SimScorer simScorer(SimWeight weight, LeafReaderContext context) throws IOException {
            return delegate.simScorer(weight, context);
        }
    }
}
