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

import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

/**
 * {@link SimilarityProvider} for {@link LMDirichletSimilarity}.
 * <p>
 * Configuration options available:
 * <ul>
 *     <li>mu</li>
 * </ul>
 * @see LMDirichletSimilarity For more information about configuration
 */
public class LMDirichletSimilarityProvider extends BaseSimilarityProvider {
    public static final Setting<Float> MU_SETTING =
        Setting.affixKeySetting("index.similarity.", ".mu", "2000f", Float::parseFloat,
            Setting.Property.IndexScope, Setting.Property.Dynamic);

    private final boolean discountOverlaps;
    private volatile float mu;

    public LMDirichletSimilarityProvider(String name, Settings settings) {
        super(name);
        this.discountOverlaps = getConcreteSetting(DISCOUNT_OVERLAPS_SETTING).get(settings);
        this.mu = getConcreteSetting(MU_SETTING).get(settings);
    }

    @Override
    public void addSettingsUpdateConsumer(IndexScopedSettings scopedSettings) {
        scopedSettings.addSettingsUpdateConsumer(getConcreteSetting(MU_SETTING), this::setMu);
    }

    private void setMu(float mu) {
        this.mu = mu;
    }

    @Override
    public LMDirichletSimilarity get() {
        LMDirichletSimilarity sim = new LMDirichletSimilarity(mu);
        sim.setDiscountOverlaps(discountOverlaps);
        return sim;
    }
}
