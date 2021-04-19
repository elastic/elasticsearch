/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.ko.KoreanTokenizerFactory;
import org.elasticsearch.indices.analysis.AnalysisFactoryTestCase;
import org.elasticsearch.plugin.analysis.nori.AnalysisNoriPlugin;

import java.util.HashMap;
import java.util.Map;

public class AnalysisNoriFactoryTests extends AnalysisFactoryTestCase {
    public AnalysisNoriFactoryTests() {
        super(new AnalysisNoriPlugin());
    }

    @Override
    protected Map<String, Class<?>> getTokenizers() {
        Map<String, Class<?>> tokenizers = new HashMap<>(super.getTokenizers());
        tokenizers.put("korean", KoreanTokenizerFactory.class);
        return tokenizers;
    }

    @Override
    protected Map<String, Class<?>> getTokenFilters() {
        Map<String, Class<?>> filters = new HashMap<>(super.getTokenFilters());
        filters.put("koreanpartofspeechstop", NoriPartOfSpeechStopFilterFactory.class);
        filters.put("koreanreadingform", NoriReadingFormFilterFactory.class);
        filters.put("koreannumber", NoriNumberFilterFactory.class);
        return filters;
    }
}
