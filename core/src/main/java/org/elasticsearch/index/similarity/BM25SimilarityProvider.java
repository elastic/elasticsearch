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
import org.apache.lucene.search.similarities.Similarity;
import org.elasticsearch.Version;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;

/**
 * {@link SimilarityProvider} for the {@link BM25Similarity}.
 * <p>
 * Configuration options available:
 * <ul>
 *     <li>k1</li>
 *     <li>b</li>
 *     <li>discount_overlaps</li>
 * </ul>
 * @see BM25Similarity For more information about configuration
 */
public class BM25SimilarityProvider extends AbstractSimilarityProvider {

    private final BM25Similarity similarity;

    public BM25SimilarityProvider(String name, Settings settings, Settings indexSettings) {
        super(name);
        float k1 = settings.getAsFloat("k1", 1.2f);
        float b = settings.getAsFloat("b", 0.75f);
        final DeprecationLogger deprecationLogger = new DeprecationLogger(ESLoggerFactory.getLogger(getClass()));
        boolean discountOverlaps =
            settings.getAsBooleanLenientForPreEs6Indices(Version.indexCreated(indexSettings), "discount_overlaps", true, deprecationLogger);

        this.similarity = new BM25Similarity(k1, b);
        this.similarity.setDiscountOverlaps(discountOverlaps);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Similarity get() {
        return similarity;
    }

}
