/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.icu;

import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RuleBasedCollator;
import com.ibm.icu.util.ULocale;

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

// Tests borrowed from Solr's Icu collation key filter factory test.
public class SimpleIcuCollationTokenFilterTests extends ESTestCase {
    /*
     * Tests usage where we do not provide a language or locale
     */
    public void testDefaultUsage() throws Exception {
        Settings settings = Settings.builder()
            .put("index.analysis.filter.myCollator.type", "icu_collation")
            .put("index.analysis.filter.myCollator.strength", "primary")
            .build();
        TestAnalysis analysis = createTestAnalysis(new Index("test", "_na_"), settings, new AnalysisICUPlugin());

        TokenFilterFactory filterFactory = analysis.tokenFilter.get("myCollator");
        assertCollatesToSame(filterFactory, "FOO", "foo");
    }

    /*
    * Turkish has some funny casing.
    * This test shows how you can solve this kind of thing easily with collation.
    * Instead of using LowerCaseFilter, use a turkish collator with primary strength.
    * Then things will sort and match correctly.
    */
    public void testBasicUsage() throws Exception {
        Settings settings = Settings.builder()
            .put("index.analysis.filter.myCollator.type", "icu_collation")
            .put("index.analysis.filter.myCollator.language", "tr")
            .put("index.analysis.filter.myCollator.strength", "primary")
            .build();
        TestAnalysis analysis = createTestAnalysis(new Index("test", "_na_"), settings, new AnalysisICUPlugin());

        TokenFilterFactory filterFactory = analysis.tokenFilter.get("myCollator");
        assertCollatesToSame(filterFactory, "I WİLL USE TURKİSH CASING", "ı will use turkish casıng");
    }

    /*
    * Test usage of the decomposition option for unicode normalization.
    */
    public void testNormalization() throws IOException {
        Settings settings = Settings.builder()
            .put("index.analysis.filter.myCollator.type", "icu_collation")
            .put("index.analysis.filter.myCollator.language", "tr")
            .put("index.analysis.filter.myCollator.strength", "primary")
            .put("index.analysis.filter.myCollator.decomposition", "canonical")
            .build();
        TestAnalysis analysis = createTestAnalysis(new Index("test", "_na_"), settings, new AnalysisICUPlugin());

        TokenFilterFactory filterFactory = analysis.tokenFilter.get("myCollator");
        assertCollatesToSame(filterFactory, "I W\u0049\u0307LL USE TURKİSH CASING", "ı will use turkish casıng");
    }

    /*
    * Test secondary strength, for english case is not significant.
    */
    public void testSecondaryStrength() throws IOException {
        Settings settings = Settings.builder()
            .put("index.analysis.filter.myCollator.type", "icu_collation")
            .put("index.analysis.filter.myCollator.language", "en")
            .put("index.analysis.filter.myCollator.strength", "secondary")
            .put("index.analysis.filter.myCollator.decomposition", "no")
            .build();
        TestAnalysis analysis = createTestAnalysis(new Index("test", "_na_"), settings, new AnalysisICUPlugin());

        TokenFilterFactory filterFactory = analysis.tokenFilter.get("myCollator");
        assertCollatesToSame(filterFactory, "TESTING", "testing");
    }

    /*
    * Setting alternate=shifted to shift whitespace, punctuation and symbols
    * to quaternary level
    */
    public void testIgnorePunctuation() throws IOException {
        Settings settings = Settings.builder()
            .put("index.analysis.filter.myCollator.type", "icu_collation")
            .put("index.analysis.filter.myCollator.language", "en")
            .put("index.analysis.filter.myCollator.strength", "primary")
            .put("index.analysis.filter.myCollator.alternate", "shifted")
            .build();
        TestAnalysis analysis = createTestAnalysis(new Index("test", "_na_"), settings, new AnalysisICUPlugin());

        TokenFilterFactory filterFactory = analysis.tokenFilter.get("myCollator");
        assertCollatesToSame(filterFactory, "foo-bar", "foo bar");
    }

    /*
    * Setting alternate=shifted and variableTop to shift whitespace, but not
    * punctuation or symbols, to quaternary level
    */
    public void testIgnoreWhitespace() throws IOException {
        Settings settings = Settings.builder()
            .put("index.analysis.filter.myCollator.type", "icu_collation")
            .put("index.analysis.filter.myCollator.language", "en")
            .put("index.analysis.filter.myCollator.strength", "primary")
            .put("index.analysis.filter.myCollator.alternate", "shifted")
            .put("index.analysis.filter.myCollator.variableTop", " ")
            .build();
        TestAnalysis analysis = createTestAnalysis(new Index("test", "_na_"), settings, new AnalysisICUPlugin());

        TokenFilterFactory filterFactory = analysis.tokenFilter.get("myCollator");
        assertCollatesToSame(filterFactory, "foo bar", "foobar");
        // now assert that punctuation still matters: foo-bar < foo bar
        assertCollation(filterFactory, "foo-bar", "foo bar", -1);
    }

    /*
    * Setting numeric to encode digits with numeric value, so that
    * foobar-9 sorts before foobar-10
    */
    public void testNumerics() throws IOException {
        Settings settings = Settings.builder()
            .put("index.analysis.filter.myCollator.type", "icu_collation")
            .put("index.analysis.filter.myCollator.language", "en")
            .put("index.analysis.filter.myCollator.numeric", "true")
            .build();
        TestAnalysis analysis = createTestAnalysis(new Index("test", "_na_"), settings, new AnalysisICUPlugin());

        TokenFilterFactory filterFactory = analysis.tokenFilter.get("myCollator");
        assertCollation(filterFactory, "foobar-9", "foobar-10", -1);
    }

    /*
    * Setting caseLevel=true to create an additional case level between
    * secondary and tertiary
    */
    public void testIgnoreAccentsButNotCase() throws IOException {
        Settings settings = Settings.builder()
            .put("index.analysis.filter.myCollator.type", "icu_collation")
            .put("index.analysis.filter.myCollator.language", "en")
            .put("index.analysis.filter.myCollator.strength", "primary")
            .put("index.analysis.filter.myCollator.caseLevel", "true")
            .build();
        TestAnalysis analysis = createTestAnalysis(new Index("test", "_na_"), settings, new AnalysisICUPlugin());

        TokenFilterFactory filterFactory = analysis.tokenFilter.get("myCollator");
        assertCollatesToSame(filterFactory, "résumé", "resume");
        assertCollatesToSame(filterFactory, "Résumé", "Resume");
        // now assert that case still matters: resume < Resume
        assertCollation(filterFactory, "resume", "Resume", -1);
    }

    /*
    * Setting caseFirst=upper to cause uppercase strings to sort
    * before lowercase ones.
    */
    public void testUpperCaseFirst() throws IOException {
        Settings settings = Settings.builder()
            .put("index.analysis.filter.myCollator.type", "icu_collation")
            .put("index.analysis.filter.myCollator.language", "en")
            .put("index.analysis.filter.myCollator.strength", "tertiary")
            .put("index.analysis.filter.myCollator.caseFirst", "upper")
            .build();
        TestAnalysis analysis = createTestAnalysis(new Index("test", "_na_"), settings, new AnalysisICUPlugin());

        TokenFilterFactory filterFactory = analysis.tokenFilter.get("myCollator");
        assertCollation(filterFactory, "Resume", "resume", -1);
    }

    /*
    * For german, you might want oe to sort and match with o umlaut.
    * This is not the default, but you can make a customized ruleset to do this.
    *
    * The default is DIN 5007-1, this shows how to tailor a collator to get DIN 5007-2 behavior.
    *  http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4423383
    */
    public void testCustomRules() throws Exception {
        RuleBasedCollator baseCollator = (RuleBasedCollator) Collator.getInstance(new ULocale("de_DE"));
        String DIN5007_2_tailorings = "& ae , a\u0308 & AE , A\u0308" + "& oe , o\u0308 & OE , O\u0308" + "& ue , u\u0308 & UE , u\u0308";

        RuleBasedCollator tailoredCollator = new RuleBasedCollator(baseCollator.getRules() + DIN5007_2_tailorings);
        String tailoredRules = tailoredCollator.getRules();

        Settings settings = Settings.builder()
            .put("index.analysis.filter.myCollator.type", "icu_collation")
            .put("index.analysis.filter.myCollator.rules", tailoredRules)
            .put("index.analysis.filter.myCollator.strength", "primary")
            .build();
        TestAnalysis analysis = createTestAnalysis(new Index("test", "_na_"), settings, new AnalysisICUPlugin());

        TokenFilterFactory filterFactory = analysis.tokenFilter.get("myCollator");
        assertCollatesToSame(filterFactory, "Töne", "Toene");
    }

    /*
     * Test a basic custom rules (should not interfere with reading rules list
     * in IcuCollationTokenFilterFactory and throw InvalidPathException on
     * Windows platforms).
     */
    public void testBasicCustomRules() throws Exception {
        Settings settings = Settings.builder()
            .put("index.analysis.filter.myCollator.type", "icu_collation")
            .put("index.analysis.filter.myCollator.rules", "&a < g")
            .build();
        TestAnalysis analysis = createTestAnalysis(new Index("test", "_na_"), settings, new AnalysisICUPlugin());

        TokenFilterFactory filterFactory = analysis.tokenFilter.get("myCollator");
        assertCollation(filterFactory, "green", "bird", -1);
    }

    private void assertCollatesToSame(TokenFilterFactory factory, String string1, String string2) throws IOException {
        assertCollation(factory, string1, string2, 0);
    }

    private void assertCollation(TokenFilterFactory factory, String string1, String string2, int comparison) throws IOException {
        Tokenizer tokenizer = new KeywordTokenizer();
        tokenizer.setReader(new StringReader(string1));
        TokenStream stream1 = factory.create(tokenizer);

        tokenizer = new KeywordTokenizer();
        tokenizer.setReader(new StringReader(string2));
        TokenStream stream2 = factory.create(tokenizer);

        assertCollation(stream1, stream2, comparison);
    }

    private void assertCollation(TokenStream stream1, TokenStream stream2, int comparison) throws IOException {
        CharTermAttribute term1 = stream1.addAttribute(CharTermAttribute.class);
        CharTermAttribute term2 = stream2.addAttribute(CharTermAttribute.class);

        stream1.reset();
        stream2.reset();

        assertThat(stream1.incrementToken(), equalTo(true));
        assertThat(stream2.incrementToken(), equalTo(true));
        assertThat(Integer.signum(term1.toString().compareTo(term2.toString())), equalTo(Integer.signum(comparison)));
        assertThat(stream1.incrementToken(), equalTo(false));
        assertThat(stream2.incrementToken(), equalTo(false));

        stream1.end();
        stream2.end();

        stream1.close();
        stream2.close();
    }
}
