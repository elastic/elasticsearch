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
import org.apache.lucene.search.similarities.Similarity;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.List;

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
    private static final Setting<Float> MU_SETTING =
        Setting.floatSetting("mu", 2000f, Setting.Property.Dynamic);

    private LMDirichletSimilarity similarity;

    public LMDirichletSimilarityProvider(String name, Settings settings) {
        super(name, settings);
        this.similarity = create(settings);
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> lst = new ArrayList<>(super.getSettings());
        lst.add(MU_SETTING);
        return lst;
    }

    @Override
    protected void doUpdateSettings(Settings settings) {
        similarity = create(settings);
    }

    @Override
    protected Similarity doGet() {
        return similarity;
    }

    private LMDirichletSimilarity create(Settings settings) {
        float mu = MU_SETTING.get(settings);
        LMDirichletSimilarity sim = new LMDirichletSimilarity(mu);
        sim.setDiscountOverlaps(discountOverlaps);
        return sim;
    }
}
