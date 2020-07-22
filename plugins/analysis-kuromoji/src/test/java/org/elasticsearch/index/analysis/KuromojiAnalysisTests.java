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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ja.JapaneseAnalyzer;
import org.apache.lucene.analysis.ja.JapaneseTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.plugin.analysis.kuromoji.AnalysisKuromojiPlugin;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.apache.lucene.analysis.BaseTokenStreamTestCase.assertTokenStreamContents;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class KuromojiAnalysisTests extends ESTestCase {
    public void testDefaultsKuromojiAnalysis() throws IOException {
        TestAnalysis analysis = createTestAnalysis();

        TokenizerFactory tokenizerFactory = analysis.tokenizer.get("kuromoji_tokenizer");
        assertThat(tokenizerFactory, instanceOf(KuromojiTokenizerFactory.class));

        TokenFilterFactory filterFactory = analysis.tokenFilter.get("kuromoji_part_of_speech");
        assertThat(filterFactory, instanceOf(KuromojiPartOfSpeechFilterFactory.class));

        filterFactory = analysis.tokenFilter.get("kuromoji_readingform");
        assertThat(filterFactory, instanceOf(KuromojiReadingFormFilterFactory.class));

        filterFactory = analysis.tokenFilter.get("kuromoji_baseform");
        assertThat(filterFactory, instanceOf(KuromojiBaseFormFilterFactory.class));

        filterFactory = analysis.tokenFilter.get("kuromoji_stemmer");
        assertThat(filterFactory, instanceOf(KuromojiKatakanaStemmerFactory.class));

        filterFactory = analysis.tokenFilter.get("ja_stop");
        assertThat(filterFactory, instanceOf(JapaneseStopTokenFilterFactory.class));

        filterFactory = analysis.tokenFilter.get("kuromoji_number");
        assertThat(filterFactory, instanceOf(KuromojiNumberFilterFactory.class));

        IndexAnalyzers indexAnalyzers = analysis.indexAnalyzers;
        NamedAnalyzer analyzer = indexAnalyzers.get("kuromoji");
        assertThat(analyzer.analyzer(), instanceOf(JapaneseAnalyzer.class));

        analyzer = indexAnalyzers.get("my_analyzer");
        assertThat(analyzer.analyzer(), instanceOf(CustomAnalyzer.class));
        assertThat(analyzer.analyzer().tokenStream(null, new StringReader("")), instanceOf(JapaneseTokenizer.class));

        CharFilterFactory  charFilterFactory = analysis.charFilter.get("kuromoji_iteration_mark");
        assertThat(charFilterFactory, instanceOf(KuromojiIterationMarkCharFilterFactory.class));

    }

    public void testBaseFormFilterFactory() throws IOException {
        TestAnalysis analysis = createTestAnalysis();
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("kuromoji_pos");
        assertThat(tokenFilter, instanceOf(KuromojiPartOfSpeechFilterFactory.class));
        String source = "私は制限スピードを超える。";
        String[] expected = new String[]{"私", "は", "制限", "スピード", "を"};
        Tokenizer tokenizer = new JapaneseTokenizer(null, true, JapaneseTokenizer.Mode.SEARCH);
        tokenizer.setReader(new StringReader(source));
        assertSimpleTSOutput(tokenFilter.create(tokenizer), expected);
    }

    public void testPartOfSpeechFilter() throws IOException {
        TestAnalysis analysis = createTestAnalysis();
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("kuromoji_part_of_speech");

        assertThat(tokenFilter, instanceOf(KuromojiPartOfSpeechFilterFactory.class));

        String source = "寿司がおいしいね";
        String[] expected_tokens = new String[]{"寿司", "おいしい"};

        Tokenizer tokenizer = new JapaneseTokenizer(null, true, JapaneseTokenizer.Mode.SEARCH);
        tokenizer.setReader(new StringReader(source));

        assertSimpleTSOutput(tokenFilter.create(tokenizer), expected_tokens);
    }

    public void testReadingFormFilterFactory() throws IOException {
        TestAnalysis analysis = createTestAnalysis();
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("kuromoji_rf");
        assertThat(tokenFilter, instanceOf(KuromojiReadingFormFilterFactory.class));
        String source = "今夜はロバート先生と話した";
        String[] expected_tokens_romaji = new String[]{"kon'ya", "ha", "robato", "sensei", "to", "hanashi", "ta"};

        Tokenizer tokenizer = new JapaneseTokenizer(null, true, JapaneseTokenizer.Mode.SEARCH);
        tokenizer.setReader(new StringReader(source));

        assertSimpleTSOutput(tokenFilter.create(tokenizer), expected_tokens_romaji);

        tokenizer = new JapaneseTokenizer(null, true, JapaneseTokenizer.Mode.SEARCH);
        tokenizer.setReader(new StringReader(source));
        String[] expected_tokens_katakana = new String[]{"コンヤ", "ハ", "ロバート", "センセイ", "ト", "ハナシ", "タ"};
        tokenFilter = analysis.tokenFilter.get("kuromoji_readingform");
        assertThat(tokenFilter, instanceOf(KuromojiReadingFormFilterFactory.class));
        assertSimpleTSOutput(tokenFilter.create(tokenizer), expected_tokens_katakana);
    }

    public void testKatakanaStemFilter() throws IOException {
        TestAnalysis analysis = createTestAnalysis();
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("kuromoji_stemmer");
        assertThat(tokenFilter, instanceOf(KuromojiKatakanaStemmerFactory.class));
        String source = "明後日パーティーに行く予定がある。図書館で資料をコピーしました。";

        Tokenizer tokenizer = new JapaneseTokenizer(null, true, JapaneseTokenizer.Mode.SEARCH);
        tokenizer.setReader(new StringReader(source));

        // パーティー should be stemmed by default
        // (min len) コピー should not be stemmed
        String[] expected_tokens_katakana = new String[] {
                "明後日", "パーティ", "に", "行く", "予定", "が", "ある", "図書館", "で", "資料", "を", "コピー", "し", "まし", "た"};
        assertSimpleTSOutput(tokenFilter.create(tokenizer), expected_tokens_katakana);

        tokenFilter = analysis.tokenFilter.get("kuromoji_ks");
        assertThat(tokenFilter, instanceOf(KuromojiKatakanaStemmerFactory.class));
        tokenizer = new JapaneseTokenizer(null, true, JapaneseTokenizer.Mode.SEARCH);
        tokenizer.setReader(new StringReader(source));

        // パーティー should not be stemmed since min len == 6
        // コピー should not be stemmed
        expected_tokens_katakana = new String[] {
                "明後日", "パーティー", "に", "行く", "予定", "が", "ある", "図書館", "で", "資料", "を", "コピー", "し", "まし", "た"};
        assertSimpleTSOutput(tokenFilter.create(tokenizer), expected_tokens_katakana);
    }

    public void testIterationMarkCharFilter() throws IOException {
        TestAnalysis analysis = createTestAnalysis();
        // test only kanji
        CharFilterFactory charFilterFactory = analysis.charFilter.get("kuromoji_im_only_kanji");
        assertNotNull(charFilterFactory);
        assertThat(charFilterFactory, instanceOf(KuromojiIterationMarkCharFilterFactory.class));

        String source = "ところゞゝゝ、ジヾが、時々、馬鹿々々しい";
        String expected = "ところゞゝゝ、ジヾが、時時、馬鹿馬鹿しい";

        assertCharFilterEquals(charFilterFactory.create(new StringReader(source)), expected);

        // test only kana

        charFilterFactory = analysis.charFilter.get("kuromoji_im_only_kana");
        assertNotNull(charFilterFactory);
        assertThat(charFilterFactory, instanceOf(KuromojiIterationMarkCharFilterFactory.class));

        expected = "ところどころ、ジジが、時々、馬鹿々々しい";

        assertCharFilterEquals(charFilterFactory.create(new StringReader(source)), expected);

        // test default

        charFilterFactory = analysis.charFilter.get("kuromoji_im_default");
        assertNotNull(charFilterFactory);
        assertThat(charFilterFactory, instanceOf(KuromojiIterationMarkCharFilterFactory.class));

        expected = "ところどころ、ジジが、時時、馬鹿馬鹿しい";

        assertCharFilterEquals(charFilterFactory.create(new StringReader(source)), expected);
    }

    public void testJapaneseStopFilterFactory() throws IOException {
        TestAnalysis analysis = createTestAnalysis();
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("ja_stop");
        assertThat(tokenFilter, instanceOf(JapaneseStopTokenFilterFactory.class));
        String source = "私は制限スピードを超える。";
        String[] expected = new String[]{"私", "制限", "超える"};
        Tokenizer tokenizer = new JapaneseTokenizer(null, true, JapaneseTokenizer.Mode.SEARCH);
        tokenizer.setReader(new StringReader(source));
        assertSimpleTSOutput(tokenFilter.create(tokenizer), expected);
    }

    private static TestAnalysis createTestAnalysis() throws IOException {
        InputStream empty_dict = KuromojiAnalysisTests.class.getResourceAsStream("empty_user_dict.txt");
        InputStream dict = KuromojiAnalysisTests.class.getResourceAsStream("user_dict.txt");
        Path home = createTempDir();
        Path config = home.resolve("config");
        Files.createDirectory(config);
        Files.copy(empty_dict, config.resolve("empty_user_dict.txt"));
        Files.copy(dict, config.resolve("user_dict.txt"));
        String json = "/org/elasticsearch/index/analysis/kuromoji_analysis.json";

        Settings settings = Settings.builder()
            .loadFromStream(json, KuromojiAnalysisTests.class.getResourceAsStream(json), false)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();
        Settings nodeSettings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), home).build();
        return createTestAnalysis(new Index("test", "_na_"), nodeSettings, settings, new AnalysisKuromojiPlugin());
    }

    public static void assertSimpleTSOutput(TokenStream stream,
                                            String[] expected) throws IOException {
        stream.reset();
        CharTermAttribute termAttr = stream.getAttribute(CharTermAttribute.class);
        assertThat(termAttr, notNullValue());
        int i = 0;
        while (stream.incrementToken()) {
            assertThat(expected.length, greaterThan(i));
            assertThat("expected different term at index " + i, termAttr.toString(), equalTo(expected[i++]));
        }
        assertThat("not all tokens produced", i, equalTo(expected.length));
    }

    private void assertCharFilterEquals(Reader filtered,
                                        String expected) throws IOException {
        String actual = readFully(filtered);
        assertThat(actual, equalTo(expected));
    }

    private String readFully(Reader reader) throws IOException {
        StringBuilder buffer = new StringBuilder();
        int ch;
        while((ch = reader.read()) != -1){
            buffer.append((char)ch);
        }
        return buffer.toString();
    }

    public void testKuromojiUserDict() throws IOException {
        TestAnalysis analysis = createTestAnalysis();
        TokenizerFactory tokenizerFactory = analysis.tokenizer.get("kuromoji_user_dict");
        String source = "私は制限スピードを超える。";
        String[] expected = new String[]{"私", "は", "制限スピード", "を", "超える"};

        Tokenizer tokenizer = tokenizerFactory.create();
        tokenizer.setReader(new StringReader(source));
        assertSimpleTSOutput(tokenizer, expected);
    }

    // fix #59
    public void testKuromojiEmptyUserDict() throws IOException {
        TestAnalysis analysis = createTestAnalysis();
        TokenizerFactory tokenizerFactory = analysis.tokenizer.get("kuromoji_empty_user_dict");
        assertThat(tokenizerFactory, instanceOf(KuromojiTokenizerFactory.class));
    }

    public void testNbestCost() throws IOException {
        TestAnalysis analysis = createTestAnalysis();
        TokenizerFactory tokenizerFactory = analysis.tokenizer.get("kuromoji_nbest_cost");
        String source = "鳩山積み";
        String[] expected = new String[] {"鳩", "鳩山", "山積み", "積み"};

        Tokenizer tokenizer = tokenizerFactory.create();
        tokenizer.setReader(new StringReader(source));
        assertSimpleTSOutput(tokenizer, expected);
    }

    public void testNbestExample() throws IOException {
        TestAnalysis analysis = createTestAnalysis();
        TokenizerFactory tokenizerFactory = analysis.tokenizer.get("kuromoji_nbest_examples");
        String source = "鳩山積み";
        String[] expected = new String[] {"鳩", "鳩山", "山積み", "積み"};

        Tokenizer tokenizer = tokenizerFactory.create();
        tokenizer.setReader(new StringReader(source));
        assertSimpleTSOutput(tokenizer, expected);
    }

    public void testNbestBothOptions() throws IOException {
        TestAnalysis analysis = createTestAnalysis();
        TokenizerFactory tokenizerFactory = analysis.tokenizer.get("kuromoji_nbest_both");
        String source = "鳩山積み";
        String[] expected = new String[] {"鳩", "鳩山", "山積み", "積み"};

        Tokenizer tokenizer = tokenizerFactory.create();
        tokenizer.setReader(new StringReader(source));
        assertSimpleTSOutput(tokenizer, expected);

    }

    public void testNumberFilterFactory() throws Exception {
        TestAnalysis analysis = createTestAnalysis();
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("kuromoji_number");
        assertThat(tokenFilter, instanceOf(KuromojiNumberFilterFactory.class));
        String source = "本日十万二千五百円のワインを買った";
        String[] expected = new String[]{"本日", "102500", "円", "の", "ワイン", "を", "買っ", "た"};
        Tokenizer tokenizer = new JapaneseTokenizer(null, true, JapaneseTokenizer.Mode.SEARCH);
        tokenizer.setReader(new StringReader(source));
        assertSimpleTSOutput(tokenFilter.create(tokenizer), expected);
    }

    public void testKuromojiAnalyzerUserDict() throws Exception {
        Settings settings = Settings.builder()
            .put("index.analysis.analyzer.my_analyzer.type", "kuromoji")
            .putList("index.analysis.analyzer.my_analyzer.user_dictionary_rules", "c++,c++,w,w", "制限スピード,制限スピード,セイゲンスピード,テスト名詞")
            .build();
        TestAnalysis analysis = createTestAnalysis(settings);
        Analyzer analyzer = analysis.indexAnalyzers.get("my_analyzer");
        try (TokenStream stream = analyzer.tokenStream("", "制限スピード")) {
            assertTokenStreamContents(stream, new String[]{"制限スピード"});
        }

        try (TokenStream stream = analyzer.tokenStream("", "c++world")) {
            assertTokenStreamContents(stream, new String[]{"c++", "world"});
        }
    }

    public void testKuromojiAnalyzerInvalidUserDictOption() throws Exception {
        Settings settings = Settings.builder()
            .put("index.analysis.analyzer.my_analyzer.type", "kuromoji")
            .put("index.analysis.analyzer.my_analyzer.user_dictionary", "user_dict.txt")
            .putList("index.analysis.analyzer.my_analyzer.user_dictionary_rules", "c++,c++,w,w")
            .build();
        IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> createTestAnalysis(settings));
        assertThat(exc.getMessage(), containsString("It is not allowed to use [user_dictionary] in conjunction " +
            "with [user_dictionary_rules]"));
    }

    public void testKuromojiAnalyzerDuplicateUserDictRule() throws Exception {
        Settings settings = Settings.builder()
            .put("index.analysis.analyzer.my_analyzer.type", "kuromoji")
            .putList("index.analysis.analyzer.my_analyzer.user_dictionary_rules",
                "c++,c++,w,w", "#comment", "制限スピード,制限スピード,セイゲンスピード,テスト名詞", "制限スピード,制限スピード,セイゲンスピード,テスト名詞")
            .build();
        IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> createTestAnalysis(settings));
        assertThat(exc.getMessage(), containsString("[制限スピード] in user dictionary at line [3]"));
    }

    public void testDiscardCompoundToken() throws Exception {
        TestAnalysis analysis = createTestAnalysis();
        TokenizerFactory tokenizerFactory = analysis.tokenizer.get("kuromoji_discard_compound_token");
        String source = "株式会社";
        String[] expected = new String[] {"株式", "会社"};

        Tokenizer tokenizer = tokenizerFactory.create();
        tokenizer.setReader(new StringReader(source));
        assertSimpleTSOutput(tokenizer, expected);
    }

    private TestAnalysis createTestAnalysis(Settings analysisSettings) throws IOException {
        InputStream dict = KuromojiAnalysisTests.class.getResourceAsStream("user_dict.txt");
        Path home = createTempDir();
        Path config = home.resolve("config");
        Files.createDirectory(config);
        Files.copy(dict, config.resolve("user_dict.txt"));
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(Environment.PATH_HOME_SETTING.getKey(), home)
            .put(analysisSettings)
            .build();
        return AnalysisTestsHelper.createTestAnalysisFromSettings(settings, new AnalysisKuromojiPlugin());
    }
}
