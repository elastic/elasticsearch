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

import java.util.function.Function;

/**
 * Abstract implementation of {@link SimilarityProvider} providing common behaviour
 */
public abstract class BaseSimilarityProvider extends SimilarityProvider {
    public static final Setting<Boolean> DISCOUNT_OVERLAPS_SETTING =
        Setting.affixKeySetting("index.similarity.", ".discount_overlaps", "true", Boolean::parseBoolean,
            Setting.Property.IndexScope);

    public static final Setting<Settings> NORMALIZATION_MODE_SETTING =
        Setting.affixKeyGroupSetting("index.similarity.", ".normalization",
            Setting.Property.IndexScope, Setting.Property.Dynamic);

    public static final Setting<String> NORMALIZATION_SETTING =
        Setting.affixKeySetting("index.similarity.", ".normalization", "no", Function.identity(),
            Setting.Property.IndexScope, Setting.Property.Dynamic);

    protected static final Normalization NO_NORMALIZATION = new Normalization.NoNormalization();

    private final String name;

    /**
     * Creates a new AbstractSimilarityProvider with the given name
     *
     * @param name Name of the Provider
     */
    protected BaseSimilarityProvider(String name) {
        this.name = name;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String name() {
        return this.name;
    }

    /**
     * Parses the given Settings and creates the appropriate {@link Normalization}
     *
     * @param settings Settings to parse
     * @return {@link Normalization} referred to in the Settings
     */
    protected Normalization parseNormalization(Settings settings) {
        String normalization = getConcreteSetting(NORMALIZATION_SETTING).get(settings);
        if (normalization.equals("no")) {
            return NO_NORMALIZATION;
        }
        Settings innerSettings = getConcreteSetting(NORMALIZATION_MODE_SETTING).get(settings);
        if (innerSettings.getAsMap().isEmpty() || innerSettings.getAsMap().size() > 1) {
            throw new IllegalArgumentException("");
        }
        if ("h1".equals(normalization)) {
            float c = innerSettings.getAsFloat("h1.c", -1f);
            return new NormalizationH1(c);
        } else if ("h2".equals(normalization)) {
            float c = innerSettings.getAsFloat("h2.c", -1f);
            return new NormalizationH2(c);
        } else if ("h3".equals(normalization)) {
            float c = innerSettings.getAsFloat("h3.c", -1f);
            return new NormalizationH3(c);
        } else if ("z".equals(normalization)) {
            float z = innerSettings.getAsFloat("z.z", -1f);
            return new NormalizationZ(z);
        }
        throw new IllegalArgumentException("Unsupported Normalization [" + innerSettings.getAsMap().keySet().iterator().next() + "]");
    }
}
