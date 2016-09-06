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

import org.apache.lucene.search.similarities.AfterEffect;
import org.apache.lucene.search.similarities.AfterEffectB;
import org.apache.lucene.search.similarities.AfterEffectL;
import org.apache.lucene.search.similarities.BasicModel;
import org.apache.lucene.search.similarities.BasicModelBE;
import org.apache.lucene.search.similarities.BasicModelD;
import org.apache.lucene.search.similarities.BasicModelG;
import org.apache.lucene.search.similarities.BasicModelIF;
import org.apache.lucene.search.similarities.BasicModelIn;
import org.apache.lucene.search.similarities.BasicModelIne;
import org.apache.lucene.search.similarities.BasicModelP;
import org.apache.lucene.search.similarities.DFRSimilarity;
import org.apache.lucene.search.similarities.Normalization;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

/**
 * {@link SimilarityProvider} for {@link DFRSimilarity}.
 * <p>
 * Configuration options available:
 * <ul>
 *     <li>basic_model</li>
 *     <li>after_effect</li>
 *     <li>normalization</li>
 * </ul>
 * @see DFRSimilarity For more information about configuration
 */
public class DFRSimilarityProvider extends BaseSimilarityProvider {
    private static final Map<String, BasicModel> BASIC_MODELS;
    private static final Map<String, AfterEffect> AFTER_EFFECTS;

    static {
        Map<String, BasicModel> models = new HashMap<>();
        models.put("be", new BasicModelBE());
        models.put("d", new BasicModelD());
        models.put("g", new BasicModelG());
        models.put("if", new BasicModelIF());
        models.put("in", new BasicModelIn());
        models.put("ine", new BasicModelIne());
        models.put("p", new BasicModelP());
        BASIC_MODELS = unmodifiableMap(models);

        Map<String, AfterEffect> effects = new HashMap<>();
        effects.put("no", new AfterEffect.NoAfterEffect());
        effects.put("b", new AfterEffectB());
        effects.put("l", new AfterEffectL());
        AFTER_EFFECTS = unmodifiableMap(effects);
    }

    public static final Setting<BasicModel> BASIC_MODEL_SETTING =
        Setting.affixKeySetting("index.similarity.", ".basic_model",
            (s) -> "be",
            (name) -> {
                BasicModel model = BASIC_MODELS.get(name);
                if (model == null) {
                    throw new IllegalArgumentException("Unsupported BasicModel [" + name + "]");
                }
                return model;
            },
            Setting.Property.IndexScope, Setting.Property.Dynamic);


    public static final Setting<AfterEffect> AFTER_EFFECT_SETTING =
        Setting.affixKeySetting("index.similarity.", ".after_effect",
            (s) -> "no",
            (name) -> {
                AfterEffect effect = AFTER_EFFECTS.get(name);
                if (effect == null) {
                    throw new IllegalArgumentException("Unsupported AfterEffect [" + name + "]");
                }
                return effect;
            },
            Setting.Property.IndexScope, Setting.Property.Dynamic);

    private final boolean discountOverlaps;
    private volatile BasicModel basicModel;
    private volatile AfterEffect afterEffect;
    private final Normalization normalization;

    public DFRSimilarityProvider(String name, Settings settings) {
        super(name);
        this.discountOverlaps = getConcreteSetting(DISCOUNT_OVERLAPS_SETTING).get(settings);
        this.basicModel = getConcreteSetting(BASIC_MODEL_SETTING).get(settings);
        this.afterEffect = getConcreteSetting(AFTER_EFFECT_SETTING).get(settings);
        this.normalization = parseNormalization(settings);
    }

    @Override
    public void addSettingsUpdateConsumer(IndexScopedSettings scopedSettings) {
        scopedSettings.addSettingsUpdateConsumer(getConcreteSetting(BASIC_MODEL_SETTING), this::setBasicModel);
        scopedSettings.addSettingsUpdateConsumer(getConcreteSetting(AFTER_EFFECT_SETTING), this::setAfterEffect);
    }

    private void setBasicModel(BasicModel model) {
        this.basicModel = model;
    }

    private void setAfterEffect(AfterEffect effect) {
        this.afterEffect = effect;
    }

    @Override
    public DFRSimilarity get() {
        DFRSimilarity sim = new DFRSimilarity(basicModel, afterEffect, normalization);
        sim.setDiscountOverlaps(discountOverlaps);
        return sim;
    }
}
