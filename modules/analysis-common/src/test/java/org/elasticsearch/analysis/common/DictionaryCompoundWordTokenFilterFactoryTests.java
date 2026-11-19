/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.index.IndexVersions.UPGRADE_TO_LUCENE_11_0_0;
import static org.hamcrest.Matchers.contains;

public class DictionaryCompoundWordTokenFilterFactoryTests extends ESTestCase {

    public void testLegacyIndexKeepsOverlappingSubwordsForOnlyLongestMatch() throws IOException {
        IndexVersion legacy = IndexVersionUtils.randomPreviousCompatibleVersion(UPGRADE_TO_LUCENE_11_0_0);
        assertThat(analyze(legacy, true, "schweinefleisch"), contains("schweinefleisch", "schwein", "wein", "fleisch"));
    }

    public void testNewIndexDropsSubwordsInsideLongestMatchForOnlyLongestMatch() throws IOException {
        IndexVersion current = IndexVersionUtils.randomVersionBetween(UPGRADE_TO_LUCENE_11_0_0, IndexVersion.current());
        assertThat(analyze(current, true, "schweinefleisch"), contains("schweinefleisch", "schwein", "fleisch"));
    }

    public void testAllMatchesEmittedOnEveryVersionWhenOnlyLongestMatchIsFalse() throws IOException {
        IndexVersion version = IndexVersionUtils.randomCompatibleVersion();
        assertThat(analyze(version, false, "schweinefleisch"), contains("schweinefleisch", "schwein", "wein", "fleisch"));
    }

    private static TokenFilterFactory factory(IndexVersion version, boolean onlyLongestMatch) throws IOException {
        Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        Settings filterSettings = Settings.builder()
            .put("type", "dictionary_decompounder")
            .putList("word_list", "schwein", "wein", "fleisch")
            .put("only_longest_match", onlyLongestMatch)
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", indexSettings);
        Environment env = TestEnvironment.newEnvironment(
            Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build()
        );
        try (CommonAnalysisPlugin plugin = new CommonAnalysisPlugin()) {
            return plugin.getTokenFilters().get("dictionary_decompounder").get(idxSettings, env, "dict", filterSettings);
        }
    }

    private static List<String> analyze(IndexVersion version, boolean onlyLongestMatch, String text) throws IOException {
        TokenFilterFactory factory = factory(version, onlyLongestMatch);
        List<String> terms = new ArrayList<>();
        try (Tokenizer tokenizer = new WhitespaceTokenizer()) {
            tokenizer.setReader(new StringReader(text));
            try (TokenStream stream = factory.create(tokenizer)) {
                CharTermAttribute term = stream.addAttribute(CharTermAttribute.class);
                stream.reset();
                while (stream.incrementToken()) {
                    terms.add(term.toString());
                }
                stream.end();
            }
        }
        return terms;
    }
}
