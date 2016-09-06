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
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
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
public class BM25SimilarityProvider extends BaseSimilarityProvider {
    public static final Setting<Float> B_SETTING = Setting.affixKeySetting("index.similarity.", ".b", "0.75f",
        (v) -> {
            float b = Float.parseFloat(v);
            if (Float.isNaN(b) || b < 0 || b > 1) {
                throw new IllegalArgumentException("illegal b value: " + b + ", must be between 0 and 1");
            }
            return b;
        },
        Setting.Property.IndexScope, Setting.Property.Dynamic);

    public static final Setting<Float> K1_SETTING = Setting.affixKeySetting("index.similarity.", ".k1", "1.2f",
        (v) -> {
            float k1 = Float.parseFloat(v);
            if (Float.isFinite(k1) == false || k1 < 0) {
                throw new IllegalArgumentException("illegal k1 value: " + k1 + ", must be a non-negative finite value");
            }
            return k1;
        },
        Setting.Property.IndexScope, Setting.Property.Dynamic);


    private volatile float b;
    private volatile float k1;
    private final boolean discountOverlaps;

    public BM25SimilarityProvider(String name, Settings settings) {
        super(name);
        Setting<Float> concreteBSetting = getConcreteSetting(B_SETTING);
        Setting<Float> concreteK1Setting = getConcreteSetting(K1_SETTING);
        this.discountOverlaps = getConcreteSetting(DISCOUNT_OVERLAPS_SETTING).get(settings);
        this.k1 = concreteK1Setting.get(settings);
        this.b = concreteBSetting.get(settings);
    }

    @Override
    public void addSettingsUpdateConsumer(IndexScopedSettings scopedSettings) {
        scopedSettings.addSettingsUpdateConsumer(getConcreteSetting(B_SETTING), this::setB);
        scopedSettings.addSettingsUpdateConsumer(getConcreteSetting(K1_SETTING), this::setK1);
    }

    private void setB(float b) {
        this.b = b;
    }

    private void setK1(float k1) {
        this.k1 = k1;
    }

    @Override
    public BM25Similarity get() {
        BM25Similarity similarity = new BM25Similarity(k1, b);
        similarity.setDiscountOverlaps(discountOverlaps);
        return similarity;
    }
}
