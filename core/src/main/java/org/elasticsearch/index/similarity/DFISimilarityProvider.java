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

import org.apache.lucene.search.similarities.DFISimilarity;
import org.apache.lucene.search.similarities.Independence;
import org.apache.lucene.search.similarities.IndependenceChiSquared;
import org.apache.lucene.search.similarities.IndependenceSaturated;
import org.apache.lucene.search.similarities.IndependenceStandardized;
import org.apache.lucene.search.similarities.Similarity;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

/**
 * {@link SimilarityProvider} for the {@link DFISimilarity}.
 * <p>
 * Configuration options available:
 * <ul>
 *     <li>independence_measure</li>
 *     <li>discount_overlaps</li>
 * </ul>
 * @see DFISimilarity For more information about configuration
 */
public class DFISimilarityProvider extends BaseSimilarityProvider {
    // the "basic models" of divergence from independence
    private static final Map<String, Independence> INDEPENDENCE_MEASURES;
    static {
        Map<String, Independence> measures = new HashMap<>();
        measures.put("standardized", new IndependenceStandardized());
        measures.put("saturated", new IndependenceSaturated());
        measures.put("chisquared", new IndependenceChiSquared());
        INDEPENDENCE_MEASURES = unmodifiableMap(measures);
    }

    private static final Setting<String> INDEPENDENCE_MEASURE_SETTING =
        Setting.simpleString("independence_measure", Setting.Property.Dynamic);
    private DFISimilarity similarity;

    public DFISimilarityProvider(String name, Settings settings) {
        super(name, settings);
        this.similarity = create(settings);
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?> > lst = new ArrayList<>(super.getSettings());
        lst.add(INDEPENDENCE_MEASURE_SETTING);
        return lst;
    }

    @Override
    protected void doValidateUpdateSettings(Settings settings) {
        create(settings);
    }

    @Override
    protected void doUpdateSettings(Settings settings) {
        similarity = create(settings);
    }

    @Override
    protected Similarity doGet() {
        return similarity;
    }

    private Independence parseIndependence(Settings settings) {
        String name = INDEPENDENCE_MEASURE_SETTING.get(settings);
        Independence measure = INDEPENDENCE_MEASURES.get(name);
        if (measure == null) {
            throw new IllegalArgumentException("Unsupported IndependenceMeasure [" + name + "]");
        }
        return measure;
    }

    private DFISimilarity create(Settings settings) {
        Independence measure = parseIndependence(settings);
        DFISimilarity sim = new DFISimilarity(measure);
        sim.setDiscountOverlaps(discountOverlaps);
        return sim;
    }
}
