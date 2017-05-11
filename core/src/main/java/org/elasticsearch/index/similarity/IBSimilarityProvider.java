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

import org.apache.lucene.search.similarities.Distribution;
import org.apache.lucene.search.similarities.DistributionLL;
import org.apache.lucene.search.similarities.DistributionSPL;
import org.apache.lucene.search.similarities.IBSimilarity;
import org.apache.lucene.search.similarities.Lambda;
import org.apache.lucene.search.similarities.LambdaDF;
import org.apache.lucene.search.similarities.LambdaTTF;
import org.apache.lucene.search.similarities.Normalization;
import org.apache.lucene.search.similarities.Similarity;
import org.elasticsearch.common.settings.Settings;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

/**
 * {@link SimilarityProvider} for {@link IBSimilarity}.
 * <p>
 * Configuration options available:
 * <ul>
 *     <li>distribution</li>
 *     <li>lambda</li>
 *     <li>normalization</li>
 * </ul>
 * @see IBSimilarity For more information about configuration
 */
public class IBSimilarityProvider extends AbstractSimilarityProvider {

    private static final Map<String, Distribution> DISTRIBUTIONS;
    private static final Map<String, Lambda> LAMBDAS;

    static {
        Map<String, Distribution> distributions = new HashMap<>();
        distributions.put("ll", new DistributionLL());
        distributions.put("spl", new DistributionSPL());
        DISTRIBUTIONS = unmodifiableMap(distributions);

        Map<String, Lambda> lamdas = new HashMap<>();
        lamdas.put("df", new LambdaDF());
        lamdas.put("ttf", new LambdaTTF());
        LAMBDAS = unmodifiableMap(lamdas);
    }

    private final IBSimilarity similarity;

    public IBSimilarityProvider(String name, Settings settings, Settings indexSettings) {
        super(name);
        Distribution distribution = parseDistribution(settings);
        Lambda lambda = parseLambda(settings);
        Normalization normalization = parseNormalization(settings);
        this.similarity = new IBSimilarity(distribution, lambda, normalization);
    }

    /**
     * Parses the given Settings and creates the appropriate {@link Distribution}
     *
     * @param settings Settings to parse
     * @return {@link Normalization} referred to in the Settings
     */
    protected Distribution parseDistribution(Settings settings) {
        String rawDistribution = settings.get("distribution");
        Distribution distribution = DISTRIBUTIONS.get(rawDistribution);
        if (distribution == null) {
            throw new IllegalArgumentException("Unsupported Distribution [" + rawDistribution + "]");
        }
        return distribution;
    }

    /**
     * Parses the given Settings and creates the appropriate {@link Lambda}
     *
     * @param settings Settings to parse
     * @return {@link Normalization} referred to in the Settings
     */
    protected Lambda parseLambda(Settings settings) {
        String rawLambda = settings.get("lambda");
        Lambda lambda = LAMBDAS.get(rawLambda);
        if (lambda == null) {
            throw new IllegalArgumentException("Unsupported Lambda [" + rawLambda + "]");
        }
        return lambda;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Similarity get() {
        return similarity;
    }
}
