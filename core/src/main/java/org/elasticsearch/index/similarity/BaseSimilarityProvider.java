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

/**
 * Abstract implementation of {@link SimilarityProvider} providing common behaviour
 */
public abstract class BaseSimilarityProvider extends SimilarityProvider {
    public static final Setting<Boolean> DISCOUNT_OVERLAPS_SETTING =
        Setting.affixKeySetting("index.similarity.", ".discount_overlaps", "true", Boolean::parseBoolean,
            Setting.Property.IndexScope);

    public static final Setting<Settings> NORMALIZATION_SETTING =
        Setting.affixKeyGroupSetting("index.similarity.", ".normalization",
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
     * @param innerSettings Settings to parse
     * @return {@link Normalization} referred to in the Settings
     */
    protected Normalization parseNormalization(Settings innerSettings) {
        if (innerSettings.get("") == null) {
            throw new IllegalArgumentException("missing normalization mode");
        }
        String normalization = innerSettings.get("");
        if ("no".equals(normalization)) {
            return NO_NORMALIZATION;
        }
        if ("h1".equals(normalization)) {
            if (innerSettings.get("h1.c") == null) {
                throw new IllegalArgumentException("missing parameter h1.c for normalization [h1]");
            }
            float c = innerSettings.getAsFloat("h1.c", -1f);
            return new NormalizationH1(c);
        } else if ("h2".equals(normalization)) {
            if (innerSettings.get("h2.c") == null) {
                throw new IllegalArgumentException("missing parameter h2.c for normalization [h2]");
            }
            float c = innerSettings.getAsFloat("h2.c", -1f);
            return new NormalizationH2(c);
        } else if ("h3".equals(normalization)) {
            if (innerSettings.get("h3.c") == null) {
            throw new IllegalArgumentException("missing parameter h3.c for normalization [h3]");
        }
            float c = innerSettings.getAsFloat("h3.c", -1f);
            return new NormalizationH3(c);
        } else if ("z".equals(normalization)) {
            if (innerSettings.get("z.z") == null) {
                throw new IllegalArgumentException("missing parameter z.z for normalization [z]");
            }
            float z = innerSettings.getAsFloat("z.z", -1f);
            return new NormalizationZ(z);
        }
        throw new IllegalArgumentException("Unsupported Normalization [" + normalization + "]");
    }
}
