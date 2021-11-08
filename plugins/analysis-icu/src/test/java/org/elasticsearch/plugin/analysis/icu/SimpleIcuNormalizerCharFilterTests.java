/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.analysis.icu;

import com.ibm.icu.text.Normalizer2;

import org.apache.lucene.analysis.CharFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.test.ESTestCase;

import java.io.StringReader;

/**
 * Test
 */
public class SimpleIcuNormalizerCharFilterTests extends ESTestCase {
    public void testDefaultSetting() throws Exception {
        Settings settings = Settings.builder().put("index.analysis.char_filter.myNormalizerChar.type", "icu_normalizer").build();
        TestAnalysis analysis = createTestAnalysis(new Index("test", "_na_"), settings, new AnalysisICUPlugin());
        CharFilterFactory charFilterFactory = analysis.charFilter.get("myNormalizerChar");

        String input = "ʰ㌰゙5℃№㈱㌘，バッファーの正規化のテスト．㋐㋑㋒㋓㋔ｶｷｸｹｺｻﾞｼﾞｽﾞｾﾞｿﾞg̈각/각நிเกषिchkʷक्षि";
        Normalizer2 normalizer = Normalizer2.getInstance(null, "nfkc_cf", Normalizer2.Mode.COMPOSE);
        String expectedOutput = normalizer.normalize(input);
        CharFilter inputReader = (CharFilter) charFilterFactory.create(new StringReader(input));
        char[] tempBuff = new char[10];
        StringBuilder output = new StringBuilder();
        while (true) {
            int length = inputReader.read(tempBuff);
            if (length == -1) break;
            output.append(tempBuff, 0, length);
            assertEquals(output.toString(), normalizer.normalize(input.substring(0, inputReader.correctOffset(output.length()))));
        }
        assertEquals(expectedOutput, output.toString());
    }

    public void testNameAndModeSetting() throws Exception {
        Settings settings = Settings.builder()
            .put("index.analysis.char_filter.myNormalizerChar.type", "icu_normalizer")
            .put("index.analysis.char_filter.myNormalizerChar.name", "nfkc")
            .put("index.analysis.char_filter.myNormalizerChar.mode", "decompose")
            .build();
        TestAnalysis analysis = createTestAnalysis(new Index("test", "_na_"), settings, new AnalysisICUPlugin());
        CharFilterFactory charFilterFactory = analysis.charFilter.get("myNormalizerChar");

        String input = "ʰ㌰゙5℃№㈱㌘，バッファーの正規化のテスト．㋐㋑㋒㋓㋔ｶｷｸｹｺｻﾞｼﾞｽﾞｾﾞｿﾞg̈각/각நிเกषिchkʷक्षि";
        Normalizer2 normalizer = Normalizer2.getInstance(null, "nfkc", Normalizer2.Mode.DECOMPOSE);
        String expectedOutput = normalizer.normalize(input);
        CharFilter inputReader = (CharFilter) charFilterFactory.create(new StringReader(input));
        char[] tempBuff = new char[10];
        StringBuilder output = new StringBuilder();
        while (true) {
            int length = inputReader.read(tempBuff);
            if (length == -1) break;
            output.append(tempBuff, 0, length);
            assertEquals(output.toString(), normalizer.normalize(input.substring(0, inputReader.correctOffset(output.length()))));
        }
        assertEquals(expectedOutput, output.toString());
    }
}
