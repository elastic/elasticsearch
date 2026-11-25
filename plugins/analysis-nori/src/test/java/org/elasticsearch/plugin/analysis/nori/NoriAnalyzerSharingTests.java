/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.nori;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.indices.analysis.SharedAnalyzerTestCase;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

/** The user dictionary is keyed on its rules, so where they come from does not matter but what they say does. */
public class NoriAnalyzerSharingTests extends SharedAnalyzerTestCase {

    private static final String TEXT = "세종시 c++ 세종 가곡역은 대한민국에 있다. 12개";
    private static final List<String> RULES = List.of("c++", "C쁠쁠", "세종", "세종시 세종 시");

    @Override
    protected AnalysisPlugin plugin() {
        return new AnalysisNoriPlugin();
    }

    private static Settings tokenizer(Settings dictionary) {
        return indexSettings(
            Settings.builder()
                .put("index.analysis.tokenizer.tok.type", "nori_tokenizer")
                .put("index.analysis.tokenizer.tok.decompound_mode", "mixed")
                .put(prefixed("index.analysis.tokenizer.tok.", dictionary))
                .put("index.analysis.filter.pos.type", "nori_part_of_speech")
                .putList("index.analysis.filter.pos.stoptags", "NR", "SP")
                .put("index.analysis.analyzer.a.tokenizer", "tok")
                .putList("index.analysis.analyzer.a.filter", "pos")
                .build()
        ).build();
    }

    private static Settings analyzer(Settings dictionary) {
        return indexSettings(
            Settings.builder()
                .put("index.analysis.analyzer.a.type", "nori")
                .put("index.analysis.analyzer.a.decompound_mode", "mixed")
                .putList("index.analysis.analyzer.a.stoptags", "NR", "SP")
                .put(prefixed("index.analysis.analyzer.a.", dictionary))
                .build()
        ).build();
    }

    private static Settings prefixed(String prefix, Settings settings) {
        return Settings.builder().put(settings).normalizePrefix(prefix).build();
    }

    private static Settings fromFile(String file) {
        return Settings.builder().put("user_dictionary", file).build();
    }

    private static Settings inline(List<String> rules) {
        return Settings.builder().putList("user_dictionary_rules", rules).build();
    }

    public void testTokenizerSharesDictionaryFromFileOrInline() throws IOException {
        Files.write(configDir().resolve("dict.txt"), RULES);
        assertSharesWithSameOutput(tokenizer(fromFile("dict.txt")), tokenizer(inline(RULES)), TEXT);
    }

    public void testAnalyzerSharesDictionaryFromFileOrInline() throws IOException {
        Files.write(configDir().resolve("dict.txt"), RULES);
        assertSharesWithSameOutput(analyzer(fromFile("dict.txt")), analyzer(inline(RULES)), TEXT);
    }

    public void testChangedDictionaryFileDoesNotShare() throws IOException {
        Files.write(configDir().resolve("dict.txt"), RULES);
        Settings fromFile = tokenizer(fromFile("dict.txt"));
        assertSharesWithSameOutput(fromFile, fromFile, TEXT);
        Files.write(configDir().resolve("other.txt"), RULES.subList(0, 2));
        assertDoesNotShare(fromFile, tokenizer(fromFile("other.txt")));
        assertDoesNotShare(analyzer(fromFile("dict.txt")), analyzer(fromFile("other.txt")));
    }

    /** Only indices created on or after NORI_DUPLICATES reject duplicate rules; rules without duplicates load the same either way. */
    public void testSharesAcrossDuplicateCheckVersions() throws IOException {
        IndexVersion old = IndexVersionUtils.getPreviousVersion(IndexVersions.NORI_DUPLICATES);
        assumeTrue(
            "no read-compatible version predates the duplicate check",
            old.onOrAfter(IndexVersionUtils.getLowestReadCompatibleVersion())
        );
        Settings oldIndex = Settings.builder().put(tokenizer(inline(RULES))).put(IndexMetadata.SETTING_VERSION_CREATED, old).build();
        assertSharesWithSameOutput(oldIndex, tokenizer(inline(RULES)), TEXT);
    }
}
