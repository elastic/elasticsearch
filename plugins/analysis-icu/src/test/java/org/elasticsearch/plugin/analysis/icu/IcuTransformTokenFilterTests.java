/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.icu;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.StringReader;

import static org.hamcrest.Matchers.equalTo;

public class IcuTransformTokenFilterTests extends ESTestCase {

    public void testBuiltInTransliterator() throws Exception {
        Settings settings = Settings.builder()
            .put("index.analysis.filter.myTransform.type", "icu_transform")
            .put("index.analysis.filter.myTransform.id", "Any-Latin")
            .build();
        TestAnalysis analysis = createTestAnalysis(new Index("test", "_na_"), settings, new AnalysisICUPlugin());
        TokenFilterFactory filterFactory = analysis.tokenFilter.get("myTransform");

        assertThat(getTransformed(filterFactory, "АБВ"), equalTo("ABV"));
    }

    public void testBuiltInTransliteratorReverse() throws Exception {
        Settings settings = Settings.builder()
            .put("index.analysis.filter.myTransform.type", "icu_transform")
            .put("index.analysis.filter.myTransform.id", "Latin-Greek")
            .put("index.analysis.filter.myTransform.dir", "reverse")
            .build();
        TestAnalysis analysis = createTestAnalysis(new Index("test", "_na_"), settings, new AnalysisICUPlugin());
        TokenFilterFactory filterFactory = analysis.tokenFilter.get("myTransform");

        // Reverse of Latin-Greek: Greek -> Latin
        assertThat(getTransformed(filterFactory, "αβγ"), equalTo("abg"));
    }

    public void testCustomRuleset() throws Exception {
        Settings settings = Settings.builder()
            .put("index.analysis.filter.myTransform.type", "icu_transform")
            .put("index.analysis.filter.myTransform.ruleset", "a > b; b > c;")
            .build();
        TestAnalysis analysis = createTestAnalysis(new Index("test", "_na_"), settings, new AnalysisICUPlugin());
        TokenFilterFactory filterFactory = analysis.tokenFilter.get("myTransform");

        assertThat(getTransformed(filterFactory, "ab"), equalTo("bc"));
    }

    public void testCustomRulesetWithDirection() throws Exception {
        Settings settings = Settings.builder()
            .put("index.analysis.filter.myTransform.type", "icu_transform")
            .put("index.analysis.filter.myTransform.ruleset", "a <> b;")
            .put("index.analysis.filter.myTransform.dir", "reverse")
            .build();
        TestAnalysis analysis = createTestAnalysis(new Index("test", "_na_"), settings, new AnalysisICUPlugin());
        TokenFilterFactory filterFactory = analysis.tokenFilter.get("myTransform");

        // Reverse of "a <> b" means b -> a
        assertThat(getTransformed(filterFactory, "b"), equalTo("a"));
    }

    public void testCustomRulesetComplexRules() throws Exception {
        Settings settings = Settings.builder()
            .put("index.analysis.filter.myTransform.type", "icu_transform")
            .put("index.analysis.filter.myTransform.ruleset", "ä > ae; ö > oe; ü > ue; Ä > Ae; Ö > Oe; Ü > Ue;")
            .build();
        TestAnalysis analysis = createTestAnalysis(new Index("test", "_na_"), settings, new AnalysisICUPlugin());
        TokenFilterFactory filterFactory = analysis.tokenFilter.get("myTransform");

        assertThat(getTransformed(filterFactory, "Ärger öffnet Überall"), equalTo("Aerger oeffnet Ueberall"));
    }

    public void testIdAndRulesetMutuallyExclusive() {
        Settings settings = Settings.builder()
            .put("index.analysis.filter.myTransform.type", "icu_transform")
            .put("index.analysis.filter.myTransform.id", "Any-Latin")
            .put("index.analysis.filter.myTransform.ruleset", "a > b;")
            .build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> createTestAnalysis(new Index("test", "_na_"), settings, new AnalysisICUPlugin())
        );
        assertThat(e.getMessage(), equalTo("icu_transform filter [myTransform] must not specify both [id] and [ruleset]"));
    }

    public void testInvalidRuleset() {
        Settings settings = Settings.builder()
            .put("index.analysis.filter.myTransform.type", "icu_transform")
            .put("index.analysis.filter.myTransform.ruleset", "not a valid > > > rule [[[")
            .build();
        expectThrows(
            IllegalArgumentException.class,
            () -> createTestAnalysis(new Index("test", "_na_"), settings, new AnalysisICUPlugin())
        );
    }

    private String getTransformed(TokenFilterFactory factory, String input) throws IOException {
        Tokenizer tokenizer = new KeywordTokenizer();
        tokenizer.setReader(new StringReader(input));
        TokenStream stream = factory.create(tokenizer);
        CharTermAttribute term = stream.addAttribute(CharTermAttribute.class);
        stream.reset();
        assertThat(stream.incrementToken(), equalTo(true));
        String result = term.toString();
        assertThat(stream.incrementToken(), equalTo(false));
        stream.end();
        stream.close();
        return result;
    }
}
