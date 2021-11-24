/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.analysis.icu;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.instanceOf;

public class SimpleIcuAnalysisTests extends ESTestCase {
    public void testDefaultsIcuAnalysis() throws IOException {
        TestAnalysis analysis = createTestAnalysis(new Index("test", "_na_"), Settings.EMPTY, new AnalysisICUPlugin());

        TokenizerFactory tokenizerFactory = analysis.tokenizer.get("icu_tokenizer");
        assertThat(tokenizerFactory, instanceOf(IcuTokenizerFactory.class));

        TokenFilterFactory filterFactory = analysis.tokenFilter.get("icu_normalizer");
        assertThat(filterFactory, instanceOf(IcuNormalizerTokenFilterFactory.class));

        filterFactory = analysis.tokenFilter.get("icu_folding");
        assertThat(filterFactory, instanceOf(IcuFoldingTokenFilterFactory.class));

        filterFactory = analysis.tokenFilter.get("icu_collation");
        assertThat(filterFactory, instanceOf(IcuCollationTokenFilterFactory.class));

        filterFactory = analysis.tokenFilter.get("icu_transform");
        assertThat(filterFactory, instanceOf(IcuTransformTokenFilterFactory.class));

        CharFilterFactory charFilterFactory = analysis.charFilter.get("icu_normalizer");
        assertThat(charFilterFactory, instanceOf(IcuNormalizerCharFilterFactory.class));
    }
}
