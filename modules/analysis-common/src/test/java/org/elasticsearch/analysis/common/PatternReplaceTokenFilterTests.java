/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.analysis.AnalysisTestsHelper;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ESTokenStreamTestCase;

import java.io.IOException;

import static org.apache.lucene.tests.analysis.BaseTokenStreamTestCase.assertTokenStreamContents;

public class PatternReplaceTokenFilterTests extends ESTokenStreamTestCase {

    public void testNormalizer() throws IOException {
        Settings settings = Settings.builder()
            .putList("index.analysis.normalizer.my_normalizer.filter", "replace_zeros")
            .put("index.analysis.filter.replace_zeros.type", "pattern_replace")
            .put("index.analysis.filter.replace_zeros.pattern", "0+")
            .put("index.analysis.filter.replace_zeros.replacement", "")
            .put("index.analysis.filter.replace_zeros.all", true)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        ESTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, new CommonAnalysisPlugin());
        assertNull(analysis.indexAnalyzers.get("my_normalizer"));
        NamedAnalyzer normalizer = analysis.indexAnalyzers.getNormalizer("my_normalizer");
        assertNotNull(normalizer);
        assertEquals("my_normalizer", normalizer.name());
        assertTokenStreamContents(normalizer.tokenStream("foo", "0000111"), new String[] { "111" });
        assertEquals(new BytesRef("111"), normalizer.normalize("foo", "0000111"));
    }

}
