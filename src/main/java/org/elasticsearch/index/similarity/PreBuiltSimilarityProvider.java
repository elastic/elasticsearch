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

import org.apache.lucene.search.similarities.Similarity;
import org.elasticsearch.common.settings.Settings;

/**
 * {@link SimilarityProvider} for pre-built Similarities
 */
public class PreBuiltSimilarityProvider extends AbstractSimilarityProvider {

    public static class Factory implements SimilarityProvider.Factory {

        private final PreBuiltSimilarityProvider similarity;

        public Factory(String name, Similarity similarity) {
            this.similarity = new PreBuiltSimilarityProvider(name, similarity);
        }

        @Override
        public SimilarityProvider create(String name, Settings settings) {
            return similarity;
        }

        public String name() {
            return similarity.name();
        }

        public SimilarityProvider get() {
            return similarity;
        }
    }

    private final Similarity similarity;

    /**
     * Creates a new {@link PreBuiltSimilarityProvider} with the given name and given
     * pre-built Similarity
     *
     * @param name Name of the Provider
     * @param similarity Pre-built Similarity
     */
    public PreBuiltSimilarityProvider(String name, Similarity similarity) {
        super(name);
        this.similarity = similarity;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Similarity get() {
        return similarity;
    }
}
