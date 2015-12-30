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

import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.elasticsearch.common.settings.Settings;

/**
 * {@link SimilarityProvider} for {@link ClassicSimilarity}.
 * <p>
 * Configuration options available:
 * <ul>
 *     <li>discount_overlaps</li>
 * </ul>
 * @see ClassicSimilarity For more information about configuration
 */
public class ClassicSimilarityProvider extends AbstractSimilarityProvider {

    private final ClassicSimilarity similarity = new ClassicSimilarity();

    public ClassicSimilarityProvider(String name, Settings settings) {
        super(name);
        boolean discountOverlaps = settings.getAsBoolean("discount_overlaps", true);
        this.similarity.setDiscountOverlaps(discountOverlaps);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ClassicSimilarity get() {
        return similarity;
    }
}
