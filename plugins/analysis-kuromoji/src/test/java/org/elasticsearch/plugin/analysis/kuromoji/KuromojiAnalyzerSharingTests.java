/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.kuromoji;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.analysis.SharedAnalyzerTestCase;
import org.elasticsearch.plugins.AnalysisPlugin;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

/** The user dictionary is keyed on its rules, so where they come from does not matter but what they say does. */
public class KuromojiAnalyzerSharingTests extends SharedAnalyzerTestCase {

    private static final String TEXT = "東京スカイツリーの最寄り駅はとうきょうスカイツリー駅です";
    private static final List<String> RULES = List.of("東京スカイツリー,東京 スカイツリー,トウキョウ スカイツリー,カスタム名詞");

    @Override
    protected AnalysisPlugin plugin() {
        return new AnalysisKuromojiPlugin();
    }

    private static Settings tokenizer(Settings dictionary) {
        return indexSettings(
            Settings.builder()
                .put("index.analysis.tokenizer.tok.type", "kuromoji_tokenizer")
                .put("index.analysis.tokenizer.tok.mode", "search")
                .put(prefixed("index.analysis.tokenizer.tok.", dictionary))
                .put("index.analysis.analyzer.a.tokenizer", "tok")
                .putList("index.analysis.analyzer.a.filter", "kuromoji_baseform")
                .build()
        ).build();
    }

    private static Settings analyzer(String type, Settings dictionary) {
        return indexSettings(
            Settings.builder().put("index.analysis.analyzer.a.type", type).put(prefixed("index.analysis.analyzer.a.", dictionary)).build()
        ).build();
    }

    private static Settings prefixed(String prefix, Settings settings) {
        return Settings.builder().put(settings).normalizePrefix(prefix).build();
    }

    private static Settings fromFile(String file) {
        return Settings.builder().put("user_dictionary", file).build();
    }

    private static Settings inline() {
        return Settings.builder().putList("user_dictionary_rules", RULES).build();
    }

    public void testTokenizerSharesDictionaryFromFileOrInline() throws IOException {
        Files.write(configDir().resolve("dict.txt"), RULES);
        assertSharesWithSameOutput(tokenizer(fromFile("dict.txt")), tokenizer(inline()), TEXT);
    }

    public void testAnalyzersShareDictionaryFromFileOrInline() throws IOException {
        Files.write(configDir().resolve("dict.txt"), RULES);
        for (String type : List.of("kuromoji", "kuromoji_completion")) {
            assertSharesWithSameOutput(analyzer(type, fromFile("dict.txt")), analyzer(type, inline()), TEXT);
        }
    }

    public void testChangedDictionaryFileDoesNotShare() throws IOException {
        Files.write(configDir().resolve("dict.txt"), RULES);
        Files.write(configDir().resolve("other.txt"), List.of("関西国際空港,関西 国際 空港,カンサイ コクサイ クウコウ,カスタム名詞"));
        assertDoesNotShare(tokenizer(fromFile("dict.txt")), tokenizer(fromFile("other.txt")));
        for (String type : List.of("kuromoji", "kuromoji_completion")) {
            assertDoesNotShare(analyzer(type, fromFile("dict.txt")), analyzer(type, fromFile("other.txt")));
        }
    }
}
