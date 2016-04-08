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

import com.ibm.icu.text.Normalizer2;
import org.apache.lucene.analysis.CharFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.plugin.analysis.icu.AnalysisICUPlugin;
import org.elasticsearch.test.ESTestCase;

import java.io.StringReader;


/**
 * Test
 */
public class SimpleIcuNormalizerCharFilterTests extends ESTestCase {
    public void testDefaultSetting() throws Exception {
        Settings settings = Settings.builder()
            .put("index.analysis.char_filter.myNormalizerChar.type", "icu_normalizer")
            .build();
        AnalysisService analysisService = createAnalysisService(new Index("test", "_na_"), settings, new AnalysisICUPlugin()::onModule);
        CharFilterFactory charFilterFactory = analysisService.charFilter("myNormalizerChar");

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
        AnalysisService analysisService = createAnalysisService(new Index("test", "_na_"), settings, new AnalysisICUPlugin()::onModule);
        CharFilterFactory charFilterFactory = analysisService.charFilter("myNormalizerChar");

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
