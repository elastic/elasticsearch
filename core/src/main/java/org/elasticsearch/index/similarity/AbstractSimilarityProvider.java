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

import org.apache.lucene.search.similarities.Normalization;
import org.apache.lucene.search.similarities.NormalizationH1;
import org.apache.lucene.search.similarities.NormalizationH2;
import org.apache.lucene.search.similarities.NormalizationH3;
import org.apache.lucene.search.similarities.NormalizationZ;
import org.elasticsearch.common.settings.Settings;

/**
 * Abstract implementation of {@link SimilarityProvider} providing common behaviour
 */
public abstract class AbstractSimilarityProvider implements SimilarityProvider {

    protected static final Normalization NO_NORMALIZATION = new Normalization.NoNormalization();

    private final String name;

    /**
     * Creates a new AbstractSimilarityProvider with the given name
     *
     * @param name Name of the Provider
     */
    protected AbstractSimilarityProvider(String name) {
        this.name = name;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String name() {
        return this.name;
    }

    /**
     * Parses the given Settings and creates the appropriate {@link Normalization}
     *
     * @param settings Settings to parse
     * @return {@link Normalization} referred to in the Settings
     */
    protected Normalization parseNormalization(Settings settings) {
        String normalization = settings.get("normalization");

        if ("no".equals(normalization)) {
            return NO_NORMALIZATION;
        } else if ("h1".equals(normalization)) {
            float c = settings.getAsFloat("normalization.h1.c", 1f);
            return new NormalizationH1(c);
        } else if ("h2".equals(normalization)) {
            float c = settings.getAsFloat("normalization.h2.c", 1f);
            return new NormalizationH2(c);
        } else if ("h3".equals(normalization)) {
            float c = settings.getAsFloat("normalization.h3.c", 800f);
            return new NormalizationH3(c);
        } else if ("z".equals(normalization)) {
            float z = settings.getAsFloat("normalization.z.z", 0.30f);
            return new NormalizationZ(z);
        } else {
            throw new IllegalArgumentException("Unsupported Normalization [" + normalization + "]");
        }
    }
}
