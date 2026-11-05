/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.icu;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.analysis.SharedAnalyzerTestCase;
import org.elasticsearch.plugins.AnalysisPlugin;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;

/**
 * ICU factories are keyed on the settings and files they are built from rather than on the ICU objects, which have identity
 * equality. Index-local filter names, including the id a ruleset transliterator is created with, must not affect output.
 */
public class IcuAnalyzerSharingTests extends SharedAnalyzerTestCase {

    private static final String TEXT = "Ελληνικά café Straße 向日葵 one-two ÅÄÖ åäö ab ba";

    @Override
    protected AnalysisPlugin plugin() {
        return new AnalysisICUPlugin();
    }

    private void copyRuleFile(String resource, String target) throws IOException {
        try (InputStream in = getClass().getResourceAsStream(resource)) {
            assertNotNull("missing test resource " + resource, in);
            Files.copy(in, configDir().resolve(target));
        }
    }

    /** A chain of every ICU component that takes settings; {@code name} prefixes the index-local filter names. */
    private static Settings chain(String name, String ruleFile) {
        String f = "index.analysis.filter." + name;
        return indexSettings(
            Settings.builder()
                .put("index.analysis.tokenizer.tok.type", "icu_tokenizer")
                .putList("index.analysis.tokenizer.tok.rule_files", "Latn:" + ruleFile)
                .put("index.analysis.char_filter.cf.type", "icu_normalizer")
                .put("index.analysis.char_filter.cf.mode", "decompose")
                .put("index.analysis.char_filter.cf.unicode_set_filter", "[^åäöÅÄÖ]")
                .put(f + "_transform.type", "icu_transform")
                .put(f + "_transform.ruleset", "a > b; b > a; ß > ss;")
                .put(f + "_normalizer.type", "icu_normalizer")
                .put(f + "_normalizer.name", "nfkc")
                .put(f + "_normalizer.unicode_set_filter", "[^åäöÅÄÖ]")
                .put(f + "_folding.type", "icu_folding")
                .put(f + "_folding.unicode_set_filter", "[^åäöÅÄÖ]")
                .put("index.analysis.analyzer.a.tokenizer", "tok")
                .putList("index.analysis.analyzer.a.char_filter", "cf")
                .putList("index.analysis.analyzer.a.filter", name + "_transform", name + "_normalizer", name + "_folding")
                .build()
        ).build();
    }

    private static Settings collation(String name, String rules) {
        return indexSettings(
            Settings.builder()
                .put("index.analysis.filter." + name + ".type", "icu_collation")
                .put("index.analysis.filter." + name + ".rules", rules)
                .put("index.analysis.filter." + name + ".strength", "primary")
                .put("index.analysis.analyzer.a.tokenizer", "standard")
                .putList("index.analysis.analyzer.a.filter", name)
                .build()
        ).build();
    }

    public void testChainSharesAcrossFilterNames() throws IOException {
        copyRuleFile("Latin-dont-break-on-hyphens.rbbi", "latin.rbbi");
        assertSharesWithSameOutput(chain("first", "latin.rbbi"), chain("second", "latin.rbbi"), TEXT);
    }

    public void testTokenizerKeyedOnRuleFileContent() throws IOException {
        copyRuleFile("Latin-dont-break-on-hyphens.rbbi", "latin.rbbi");
        copyRuleFile("Latin-dont-break-on-hyphens.rbbi", "latin-copy.rbbi");
        copyRuleFile("KeywordTokenizer.rbbi", "keyword.rbbi");
        assertSharesWithSameOutput(chain("first", "latin.rbbi"), chain("first", "latin-copy.rbbi"), TEXT);
        assertDoesNotShare(chain("first", "latin.rbbi"), chain("first", "keyword.rbbi"));
    }

    /** Collation rules name a file in the config directory, or are the rules themselves if no such file resolves. */
    public void testCollationSharesRulesFromFileOrInline() throws IOException {
        Files.writeString(configDir().resolve("collation.txt"), "&a < b");
        Files.writeString(configDir().resolve("other.txt"), "&b < a");
        assertSharesWithSameOutput(collation("first", "collation.txt"), collation("second", "&a < b"), "ab ba b a");
        assertDoesNotShare(collation("first", "collation.txt"), collation("first", "other.txt"));
    }
}
