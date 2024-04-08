/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.analysis.icu;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.tests.analysis.BaseTokenStreamTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class IcuAnalyzerTests extends BaseTokenStreamTestCase {

    public void testMixedAlphabetTokenization() throws IOException {

        String input = "안녕은하철도999극장판2.1981년8월8일.일본개봉작1999년재더빙video판";

        AnalysisICUPlugin plugin = new AnalysisICUPlugin();
        Analyzer analyzer = plugin.getAnalyzers().get("icu_analyzer").get(null, null, "icu", Settings.EMPTY).get();
        assertAnalyzesTo(
            analyzer,
            input,
            new String[] { "안녕은하철도", "999", "극장판", "2.1981", "년", "8", "월", "8", "일", "일본개봉작", "1999", "년재더빙", "video", "판" }
        );

    }

    public void testMiddleDots() throws IOException {
        String input = "경승지·산악·협곡·해협·곶·심연·폭포·호수·급류";

        Analyzer analyzer = new IcuAnalyzerProvider(null, null, "icu", Settings.EMPTY).get();
        assertAnalyzesTo(analyzer, input, new String[] { "경승지", "산악", "협곡", "해협", "곶", "심연", "폭포", "호수", "급류" });
    }

    public void testUnicodeNumericCharacters() throws IOException {
        String input = "① ② ③ ⑴ ⑵ ⑶ ¼ ⅓ ⅜ ¹ ² ³ ₁ ₂ ₃";

        Analyzer analyzer = new IcuAnalyzerProvider(null, null, "icu", Settings.EMPTY).get();
        assertAnalyzesTo(analyzer, input, new String[] { "1", "2", "3", "1", "2", "3", "1/4", "1/3", "3/8", "1", "2", "3", "1", "2", "3" });
    }

    public void testBadSettings() {

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put("mode", "wrong")
            .build();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new IcuAnalyzerProvider(null, null, "icu", settings)
        );

        assertThat(e.getMessage(), containsString("Unknown mode [wrong] in analyzer [icu], expected one of [compose, decompose]"));

    }

}
