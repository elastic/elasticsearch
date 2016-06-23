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
import org.apache.lucene.search.similarities.Similarity;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


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
    private BM25Similarity similarity;
    private static Setting<Float> K1_SETTING =
        Setting.floatSetting("k1", 1.2f, 0f, Setting.Property.Dynamic);
    private static Setting<Float> B_SETTING =
        Setting.floatSetting("b", 0.75f, 0f, 1f, Setting.Property.Dynamic);

    public BM25SimilarityProvider(String name, Settings settings) {
        super(name, settings);
        this.similarity = create(settings);
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?> > lst = new ArrayList<>(super.getSettings());
        lst.addAll(Arrays.asList(K1_SETTING, B_SETTING));
        return lst;
    }


    @Override
    protected void doUpdateSettings(Settings settings) {
        this.similarity = create(settings);
    }

    @Override
    protected Similarity doGet() {
        return similarity;
    }

    private BM25Similarity create(Settings settings) {
        float k1 = K1_SETTING.get(settings);
        float b = B_SETTING.get(settings);
        BM25Similarity sim = new BM25Similarity(k1, b);
        sim.setDiscountOverlaps(discountOverlaps);
        return sim;
    }
}
