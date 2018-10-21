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

package org.elasticsearch.index.analysis;

import org.elasticsearch.indices.analysis.AnalysisFactoryTestCase;
import org.elasticsearch.plugin.analysis.icu.AnalysisICUPlugin;

import java.util.HashMap;
import java.util.Map;

public class AnalysisICUFactoryTests extends AnalysisFactoryTestCase {
    public AnalysisICUFactoryTests() {
        super(new AnalysisICUPlugin());
    }

    @Override
    protected Map<String, Class<?>> getTokenizers() {
        Map<String, Class<?>> tokenizers = new HashMap<>(super.getTokenizers());
        tokenizers.put("icu", IcuTokenizerFactory.class);
        return tokenizers;
    }

    @Override
    protected Map<String, Class<?>> getTokenFilters() {
        Map<String, Class<?>> filters = new HashMap<>(super.getTokenFilters());
        filters.put("icufolding", IcuFoldingTokenFilterFactory.class);
        filters.put("icunormalizer2", IcuNormalizerTokenFilterFactory.class);
        filters.put("icutransform", IcuTransformTokenFilterFactory.class);
        return filters;
    }

    @Override
    protected Map<String, Class<?>> getCharFilters() {
        Map<String, Class<?>> filters = new HashMap<>(super.getCharFilters());
        filters.put("icunormalizer2", IcuNormalizerCharFilterFactory.class);
        return filters;
    }

}
