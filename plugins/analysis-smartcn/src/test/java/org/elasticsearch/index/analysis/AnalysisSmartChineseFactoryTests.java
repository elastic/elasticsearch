/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.elasticsearch.indices.analysis.AnalysisFactoryTestCase;
import org.elasticsearch.plugin.analysis.smartcn.AnalysisSmartChinesePlugin;

import java.util.HashMap;
import java.util.Map;

public class AnalysisSmartChineseFactoryTests extends AnalysisFactoryTestCase {
    public AnalysisSmartChineseFactoryTests() {
        super(new AnalysisSmartChinesePlugin());
    }
    @Override
    protected Map<String, Class<?>> getTokenizers() {
        Map<String, Class<?>> tokenizers = new HashMap<>(super.getTokenizers());
        tokenizers.put("hmmchinese", SmartChineseTokenizerTokenizerFactory.class);
        return tokenizers;
    }

}
