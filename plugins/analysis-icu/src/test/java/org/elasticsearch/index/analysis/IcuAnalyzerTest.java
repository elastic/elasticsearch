package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.IndexSettingsModule;

import java.io.IOException;

public class IcuAnalyzerTest extends BaseTokenStreamTestCase {

    public void testMixedAlphabetTokenization() throws IOException {

        Settings settings = Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);

        String input = "안녕은하철도999극장판2.1981년8월8일.일본개봉작1999년재더빙video판";

        Analyzer analyzer = new IcuAnalyzerProvider(idxSettings, null, "icu", settings).get();
        assertAnalyzesTo(analyzer, input,
            new String[]{"안녕은하철도", "999", "극장판", "2.1981", "년", "8", "월", "8", "일", "일본개봉작", "1999", "년재더빙", "video", "판"});

    }

    public void testMiddleDots() throws IOException {
        Settings settings = Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);

        String input = "경승지·산악·협곡·해협·곶·심연·폭포·호수·급류";

        Analyzer analyzer = new IcuAnalyzerProvider(idxSettings, null, "icu", settings).get();
        assertAnalyzesTo(analyzer, input,
            new String[]{"경승지", "산악", "협곡", "해협", "곶", "심연", "폭포", "호수", "급류"});
    }

    public void testUnicodeNumericCharacters() throws IOException {

        Settings settings = Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);

        String input = "① ② ③ ⑴ ⑵ ⑶ ¼ ⅓ ⅜ ¹ ² ³ ₁ ₂ ₃";

        Analyzer analyzer = new IcuAnalyzerProvider(idxSettings, null, "icu", settings).get();
        assertAnalyzesTo(analyzer, input,
            new String[]{"1", "2", "3", "1", "2", "3", "1/4", "1/3", "3/8", "1", "2", "3", "1", "2", "3"});
    }

    public void testZeroWidthNonJoiners() throws IOException {

    }
}
