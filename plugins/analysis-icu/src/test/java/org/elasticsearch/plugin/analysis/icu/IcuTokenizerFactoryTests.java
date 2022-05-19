/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.analysis.icu;

import org.apache.lucene.analysis.icu.segmentation.ICUTokenizer;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.apache.lucene.tests.analysis.BaseTokenStreamTestCase.assertTokenStreamContents;

public class IcuTokenizerFactoryTests extends ESTestCase {

    public void testSimpleIcuTokenizer() throws IOException {
        TestAnalysis analysis = createTestAnalysis();

        TokenizerFactory tokenizerFactory = analysis.tokenizer.get("icu_tokenizer");
        ICUTokenizer tokenizer = (ICUTokenizer) tokenizerFactory.create();

        Reader reader = new StringReader("向日葵, one-two");
        tokenizer.setReader(reader);
        assertTokenStreamContents(tokenizer, new String[] { "向日葵", "one", "two" });
    }

    public void testIcuCustomizeRuleFile() throws IOException {
        TestAnalysis analysis = createTestAnalysis();

        // test the tokenizer with single rule file
        TokenizerFactory tokenizerFactory = analysis.tokenizer.get("user_rule_tokenizer");
        ICUTokenizer tokenizer = (ICUTokenizer) tokenizerFactory.create();
        Reader reader = new StringReader("One-two punch.  Brang-, not brung-it.  This one--not that one--is the right one, -ish.");

        tokenizer.setReader(reader);
        assertTokenStreamContents(
            tokenizer,
            new String[] {
                "One-two",
                "punch",
                "Brang",
                "not",
                "brung-it",
                "This",
                "one",
                "not",
                "that",
                "one",
                "is",
                "the",
                "right",
                "one",
                "ish" }
        );
    }

    public void testMultipleIcuCustomizeRuleFiles() throws IOException {
        TestAnalysis analysis = createTestAnalysis();

        // test the tokenizer with two rule files
        TokenizerFactory tokenizerFactory = analysis.tokenizer.get("multi_rule_tokenizer");
        ICUTokenizer tokenizer = (ICUTokenizer) tokenizerFactory.create();
        StringReader reader = new StringReader("Some English.  Немного русский.  ข้อความภาษาไทยเล็ก ๆ น้อย ๆ  More English.");

        tokenizer.setReader(reader);
        assertTokenStreamContents(
            tokenizer,
            new String[] { "Some", "English", "Немного русский.  ", "ข้อความภาษาไทยเล็ก ๆ น้อย ๆ  ", "More", "English" }
        );
    }

    private static TestAnalysis createTestAnalysis() throws IOException {
        InputStream keywords = IcuTokenizerFactoryTests.class.getResourceAsStream("KeywordTokenizer.rbbi");
        InputStream latin = IcuTokenizerFactoryTests.class.getResourceAsStream("Latin-dont-break-on-hyphens.rbbi");

        Path home = createTempDir();
        Path config = home.resolve("config");
        Files.createDirectory(config);
        Files.copy(keywords, config.resolve("KeywordTokenizer.rbbi"));
        Files.copy(latin, config.resolve("Latin-dont-break-on-hyphens.rbbi"));

        String json = "/org/elasticsearch/plugin/analysis/icu/icu_analysis.json";

        Settings settings = Settings.builder()
            .loadFromStream(json, IcuTokenizerFactoryTests.class.getResourceAsStream(json), false)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();
        Settings nodeSettings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), home).build();

        return createTestAnalysis(new Index("test", "_na_"), nodeSettings, settings, new AnalysisICUPlugin());
    }
}
