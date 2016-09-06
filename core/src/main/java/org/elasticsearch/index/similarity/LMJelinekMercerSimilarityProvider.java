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

import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

/**
 * {@link SimilarityProvider} for {@link LMJelinekMercerSimilarity}.
 * <p>
 * Configuration options available:
 * <ul>
 *     <li>lambda</li>
 * </ul>
 * @see LMJelinekMercerSimilarity For more information about configuration
 */
public class LMJelinekMercerSimilarityProvider extends BaseSimilarityProvider {
    public static final Setting<Float> LAMBDA_SETTING =
        Setting.affixKeySetting("index.similarity.", ".lambda", "0.1f", Float::parseFloat,
            Setting.Property.IndexScope, Setting.Property.Dynamic);

    private final boolean discountOverlaps;
    private volatile float lambda;


    public LMJelinekMercerSimilarityProvider(String name, Settings settings) {
        super(name);
        this.discountOverlaps = getConcreteSetting(DISCOUNT_OVERLAPS_SETTING).get(settings);
        this.lambda = getConcreteSetting(LAMBDA_SETTING).get(settings);
    }

    @Override
    public void addSettingsUpdateConsumer(IndexScopedSettings scopedSettings) {
        scopedSettings.addSettingsUpdateConsumer(getConcreteSetting(LAMBDA_SETTING), this::setLambda);
    }

    private void setLambda(float lambda) {
        this.lambda = lambda;
    }

    @Override
    public LMJelinekMercerSimilarity get() {
        LMJelinekMercerSimilarity sim = new LMJelinekMercerSimilarity(lambda);
        sim.setDiscountOverlaps(discountOverlaps);
        return sim;
    }
}
