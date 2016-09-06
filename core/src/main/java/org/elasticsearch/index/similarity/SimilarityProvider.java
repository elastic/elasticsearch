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

import org.apache.lucene.search.similarities.Similarity;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;

/**
 * Provider for {@link Similarity} instances
 */
public abstract class SimilarityProvider {
    /**
     * Returns the name associated with the Provider
     *
     * @return Name of the Provider
     */
    public abstract String name();

    /**
     * Returns the {@link Similarity} the Provider is for
     *
     * @return Provided {@link Similarity}
     */
    public abstract Similarity get();


    /**
     * Allows to add update consumer for the settings of the similarity provider
     */
    public abstract void addSettingsUpdateConsumer(IndexScopedSettings scopedSettings);

    /**
     * Returns the concrete setting translated from the provided {@link Setting}
     *
     * @return The concrete setting for the provided {@link Setting}
     */
    protected <T> Setting<T> getConcreteSetting(Setting<T> setting) {
        if (setting.getRawKey() instanceof Setting.AffixKey == false) {
            throw new IllegalArgumentException("setting must be an affix setting");
        }
        Setting.AffixKey k = (Setting.AffixKey) setting.getRawKey();
        String fullKey = k.toConcreteKey(name()).toString();
        return setting.getConcreteSetting(fullKey);
    }
}
