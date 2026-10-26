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

    private static Settings tokenizer(String dictionarySetting, Object dictionary) {
        return indexSettings(
            dictionary(
                Settings.builder()
                    .put("index.analysis.tokenizer.tok.type", "nori_tokenizer")
                    .put("index.analysis.tokenizer.tok.decompound_mode", "mixed")
                    .put("index.analysis.filter.pos.type", "nori_part_of_speech")
                    .putList("index.analysis.filter.pos.stoptags", "NR", "SP")
                    .put("index.analysis.analyzer.a.tokenizer", "tok")
                    .putList("index.analysis.analyzer.a.filter", "pos"),
                "index.analysis.tokenizer.tok." + dictionarySetting,
                dictionary
            ).build()
        ).build();
    }

    private static Settings analyzer(String dictionarySetting, Object dictionary) {
        return indexSettings(
            dictionary(
                Settings.builder()
                    .put("index.analysis.analyzer.a.type", "nori")
                    .put("index.analysis.analyzer.a.decompound_mode", "mixed")
                    .putList("index.analysis.analyzer.a.stoptags", "NR", "SP"),
                "index.analysis.analyzer.a." + dictionarySetting,
                dictionary
            ).build()
        ).build();
    }

    @SuppressWarnings("unchecked")
    private static Settings.Builder dictionary(Settings.Builder builder, String key, Object dictionary) {
        return dictionary instanceof List<?> rules ? builder.putList(key, (List<String>) rules) : builder.put(key, (String) dictionary);
    }

    public void testTokenizerSharesDictionaryFromFileOrInline() throws IOException {
        Files.write(configDir().resolve("dict.txt"), RULES);
        assertSharesWithSameOutput(tokenizer("user_dictionary", "dict.txt"), tokenizer("user_dictionary_rules", RULES), TEXT);
    }

    public void testAnalyzerSharesDictionaryFromFileOrInline() throws IOException {
        Files.write(configDir().resolve("dict.txt"), RULES);
        assertSharesWithSameOutput(analyzer("user_dictionary", "dict.txt"), analyzer("user_dictionary_rules", RULES), TEXT);
    }

    public void testChangedDictionaryFileDoesNotShare() throws IOException {
        Files.write(configDir().resolve("dict.txt"), RULES);
        Settings fromFile = tokenizer("user_dictionary", "dict.txt");
        assertSharesWithSameOutput(fromFile, fromFile, TEXT);
        Files.write(configDir().resolve("other.txt"), RULES.subList(0, 2));
        assertDoesNotShare(fromFile, tokenizer("user_dictionary", "other.txt"));
        assertDoesNotShare(analyzer("user_dictionary", "dict.txt"), analyzer("user_dictionary", "other.txt"));
    }

    /** Only indices created on or after NORI_DUPLICATES reject duplicate rules; rules without duplicates load the same either way. */
    public void testSharesAcrossDuplicateCheckVersions() throws IOException {
        IndexVersion old = IndexVersionUtils.getPreviousVersion(IndexVersions.NORI_DUPLICATES);
        assumeTrue(
            "no read-compatible version predates the duplicate check",
            old.onOrAfter(IndexVersionUtils.getLowestReadCompatibleVersion())
        );
        Settings oldIndex = Settings.builder()
            .put(tokenizer("user_dictionary_rules", RULES))
            .put(IndexMetadata.SETTING_VERSION_CREATED, old)
            .build();
        assertSharesWithSameOutput(oldIndex, tokenizer("user_dictionary_rules", RULES), TEXT);
    }
}
