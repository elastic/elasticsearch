/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.analysis.phonetic;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.phonetic.DaitchMokotoffSoundexFilter;
import org.apache.lucene.tests.analysis.BaseTokenStreamTestCase;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.MatcherAssert;
import org.junit.Before;

import java.io.IOException;
import java.io.StringReader;

import static org.hamcrest.Matchers.instanceOf;

public class SimplePhoneticAnalysisTests extends ESTestCase {

    private TestAnalysis analysis;

    @Before
    public void setup() throws IOException {
        String yaml = "/org/elasticsearch/plugin/analysis/phonetic/phonetic-1.yml";
        Settings settings = Settings.builder()
            .loadFromStream(yaml, getClass().getResourceAsStream(yaml), false)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();
        this.analysis = createTestAnalysis(new Index("test", "_na_"), settings, new AnalysisPhoneticPlugin());
    }

    public void testPhoneticTokenFilterFactory() throws IOException {
        TokenFilterFactory filterFactory = analysis.tokenFilter.get("phonetic");
        MatcherAssert.assertThat(filterFactory, instanceOf(PhoneticTokenFilterFactory.class));
    }

    public void testPhoneticTokenFilterBeiderMorseNoLanguage() throws IOException {
        TokenFilterFactory filterFactory = analysis.tokenFilter.get("beidermorsefilter");
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader("ABADIAS"));
        String[] expected = new String[] {
            "abYdias",
            "abYdios",
            "abadia",
            "abadiaS",
            "abadias",
            "abadio",
            "abadioS",
            "abadios",
            "abodia",
            "abodiaS",
            "abodias",
            "abodio",
            "abodioS",
            "abodios",
            "avadias",
            "avadios",
            "avodias",
            "avodios",
            "obadia",
            "obadiaS",
            "obadias",
            "obadio",
            "obadioS",
            "obadios",
            "obodia",
            "obodiaS",
            "obodias",
            "obodioS" };
        BaseTokenStreamTestCase.assertTokenStreamContents(filterFactory.create(tokenizer), expected);
    }

    public void testPhoneticTokenFilterBeiderMorseWithLanguage() throws IOException {
        TokenFilterFactory filterFactory = analysis.tokenFilter.get("beidermorsefilterfrench");
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader("Rimbault"));
        String[] expected = new String[] {
            "rimbD",
            "rimbDlt",
            "rimba",
            "rimbalt",
            "rimbo",
            "rimbolt",
            "rimbu",
            "rimbult",
            "rmbD",
            "rmbDlt",
            "rmba",
            "rmbalt",
            "rmbo",
            "rmbolt",
            "rmbu",
            "rmbult" };
        BaseTokenStreamTestCase.assertTokenStreamContents(filterFactory.create(tokenizer), expected);
    }

    public void testPhoneticTokenFilterDaitchMotokoff() throws IOException {
        TokenFilterFactory filterFactory = analysis.tokenFilter.get("daitch_mokotoff");
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader("chauptman"));
        String[] expected = new String[] { "473660", "573660" };
        assertThat(filterFactory.create(tokenizer), instanceOf(DaitchMokotoffSoundexFilter.class));
        BaseTokenStreamTestCase.assertTokenStreamContents(filterFactory.create(tokenizer), expected);
    }

}
