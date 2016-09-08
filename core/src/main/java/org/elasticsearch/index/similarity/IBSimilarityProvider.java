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
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
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
public class IBSimilarityProvider extends BaseSimilarityProvider {
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

    public static final Setting<Distribution> DISTRIBUTION_SETTING =
        Setting.affixKeySetting("index.similarity.", ".distribution",
            (s) -> "ll",
            (name) -> {
                Distribution distrib = DISTRIBUTIONS.get(name);
                if (distrib == null) {
                    throw new IllegalArgumentException("Unsupported Distribution [" + name + "]");
                }
                return distrib;
            },
            Setting.Property.IndexScope, Setting.Property.Dynamic);

    public static final Setting<Lambda> COLLECTION_MODEL_SETTING =
        Setting.affixKeySetting("index.similarity.", ".collection_model",
            (s) -> "df",
            (name) -> {
                Lambda lambda = LAMBDAS.get(name);
                if (lambda == null) {
                    throw new IllegalArgumentException("Unsupported CollectionModel [" + name + "]");
                }
                return lambda;
            },
            Setting.Property.IndexScope, Setting.Property.Dynamic);

    private final boolean discountOverlaps;
    private volatile Normalization normalization;
    private volatile Distribution distribution;
    private volatile Lambda lambda;

    public IBSimilarityProvider(String name, Settings settings) {
        super(name);
        this.discountOverlaps = getConcreteSetting(DISCOUNT_OVERLAPS_SETTING).get(settings);
        this.distribution = getConcreteSetting(DISTRIBUTION_SETTING).get(settings);
        this.lambda = getConcreteSetting(COLLECTION_MODEL_SETTING).get(settings);
        this.normalization =  parseNormalization(getConcreteSetting(NORMALIZATION_SETTING).get(settings));
    }

    @Override
    public void addSettingsUpdateConsumer(IndexScopedSettings scopedSettings) {
        scopedSettings.addSettingsUpdateConsumer(getConcreteSetting(DISTRIBUTION_SETTING), this::setDistribution);
        scopedSettings.addSettingsUpdateConsumer(getConcreteSetting(COLLECTION_MODEL_SETTING), this::setCollectionModel);
        scopedSettings.addSettingsUpdateConsumer(getConcreteSetting(NORMALIZATION_SETTING), this::setNormalization);
    }

    private void setDistribution(Distribution distribution) {
        this.distribution = distribution;
    }

    private void setCollectionModel(Lambda lambda) {
        this.lambda = lambda;
    }

    private void setNormalization(Settings settings) {
        this.normalization = parseNormalization(settings);
    }

    @Override
    public IBSimilarity get() {
        IBSimilarity sim = new IBSimilarity(distribution, lambda, normalization);
        sim.setDiscountOverlaps(discountOverlaps);
        return sim;
    }
}
