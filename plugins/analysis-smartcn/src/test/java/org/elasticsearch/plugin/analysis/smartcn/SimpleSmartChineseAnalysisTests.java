/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.analysis.smartcn;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.MatcherAssert;

import java.io.IOException;

import static org.hamcrest.Matchers.instanceOf;

public class SimpleSmartChineseAnalysisTests extends ESTestCase {
    public void testDefaultsIcuAnalysis() throws IOException {
        final TestAnalysis analysis = createTestAnalysis(new Index("test", "_na_"), Settings.EMPTY, new AnalysisSmartChinesePlugin());
        TokenizerFactory tokenizerFactory = analysis.tokenizer.get("smartcn_tokenizer");
        MatcherAssert.assertThat(tokenizerFactory, instanceOf(SmartChineseTokenizerTokenizerFactory.class));
    }
}
