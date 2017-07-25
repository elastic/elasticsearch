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

import org.apache.lucene.search.similarities.BooleanSimilarity;
import org.elasticsearch.common.settings.Settings;

/**
 * {@link SimilarityProvider} for the {@link BooleanSimilarity},
 * which is a simple similarity that gives terms a score equal
 * to their query boost only.  This is useful in situations where
 * a field does not need to be scored by a full-text ranking
 * algorithm, but rather all that matters is whether the query
 * terms matched or not.
 */
public class BooleanSimilarityProvider extends AbstractSimilarityProvider {

    private final BooleanSimilarity similarity = new BooleanSimilarity();

    public BooleanSimilarityProvider(String name, Settings settings, Settings indexSettings) {
        super(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BooleanSimilarity get() {
        return similarity;
    }
}
