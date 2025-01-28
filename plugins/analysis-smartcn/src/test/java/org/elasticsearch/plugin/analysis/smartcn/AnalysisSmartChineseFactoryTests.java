/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.smartcn;

import org.elasticsearch.indices.analysis.AnalysisFactoryTestCase;

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
