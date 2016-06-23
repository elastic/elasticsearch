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
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.util.List;
import java.util.Arrays;


/**
 * Base implementation of {@link SimilarityProvider} that encodes the document length in the same way.
 */
public abstract class BaseSimilarityProvider extends SimilarityProvider {
    protected static final Normalization NO_NORMALIZATION = new Normalization.NoNormalization();

    protected static final Setting<Boolean> DISCOUNT_OVERLAPS_SETTING = Setting.boolSetting("discount_overlaps", true);
    protected static final Setting<String> NORMALIZATION_SETTING = new Setting<> ("normalization", (s) -> "no",
        (s) -> {
            if ("no".equals(s) || "h1".equals(s) || "h2".equals(s) || "h3".equals(s) || "z".equals(s)) {
                return s;
            }
            throw new IllegalArgumentException("Unsupported Normalization [" + s + "]");
        }, Setting.Property.Dynamic);
    protected static final Setting<Float> H1_C_SETTING = Setting.floatSetting("normalization.h1.c", 1f, Setting.Property.Dynamic);
    protected static final Setting<Float> H2_C_SETTING = Setting.floatSetting("normalization.h2.c", 1f, Setting.Property.Dynamic);
    protected static final Setting<Float> H3_C_SETTING = Setting.floatSetting("normalization.h3.c", 800f, Setting.Property.Dynamic);
    protected static final Setting<Float> Z_Z_SETTING = Setting.floatSetting("normalization.z.z", 0.3f, Setting.Property.Dynamic);

    protected final String name;
    protected final boolean discountOverlaps;

    /**
     * Creates a new AbstractSimilarityProvider with the given name
     *
     * @param name Name of the Provider
     */
    protected BaseSimilarityProvider(String name, Settings settings) {
        super(name, settings);
        this.name = name;
        this.discountOverlaps = DISCOUNT_OVERLAPS_SETTING.get(settings);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(DISCOUNT_OVERLAPS_SETTING);
    }

    /**
     * Parses the given Settings and creates the appropriate {@link Normalization}
     *
     * @param settings Settings to parse
     * @return {@link Normalization} referred to in the Settings
     */
    protected static Normalization parseNormalization(Settings settings) {
        String normalization = NORMALIZATION_SETTING.get(settings);

        if ("no".equals(normalization)) {
            return NO_NORMALIZATION;
        } else if ("h1".equals(normalization)) {
            float c = H1_C_SETTING.get(settings);
            return new NormalizationH1(c);
        } else if ("h2".equals(normalization)) {
            float c = H2_C_SETTING.get(settings);
            return new NormalizationH2(c);
        } else if ("h3".equals(normalization)) {
            float c = H3_C_SETTING.get(settings);
            return new NormalizationH3(c);
        } else if ("z".equals(normalization)) {
            float z = Z_Z_SETTING.get(settings);
            return new NormalizationZ(z);
        } else {
            throw new IllegalArgumentException("Unsupported Normalization [" + normalization + "]");
        }
    }
}
