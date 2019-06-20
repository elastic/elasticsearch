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
package org.elasticsearch.search.suggest.phrase;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.reverse.ReverseStringFilter;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.synonym.SolrSynonymParser;
import org.apache.lucene.analysis.synonym.SynonymFilter;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.search.spell.DirectSpellChecker;
import org.apache.lucene.search.spell.SuggestMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.elasticsearch.search.suggest.phrase.NoisyChannelSpellChecker.Result;
import org.elasticsearch.test.ESTestCase;

import java.io.CharArrayReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.search.suggest.phrase.NoisyChannelSpellChecker.DEFAULT_TOKEN_LIMIT;
import static org.elasticsearch.search.suggest.phrase.NoisyChannelSpellChecker.REAL_WORD_LIKELIHOOD;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class NoisyChannelSpellCheckerTests extends ESTestCase {
    private final BytesRef space = new BytesRef(" ");
    private final BytesRef preTag = new BytesRef("<em>");
    private final BytesRef postTag = new BytesRef("</em>");

    public void testNgram() throws IOException {
        RAMDirectory dir = new RAMDirectory();
        Map<String, Analyzer> mapping = new HashMap<>();
        mapping.put("body_ngram", new Analyzer() {

            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer t = new StandardTokenizer();
                ShingleFilter tf = new ShingleFilter(t, 2, 3);
                tf.setOutputUnigrams(false);
                return new TokenStreamComponents(t, new LowerCaseFilter(tf));
            }

        });

        mapping.put("body", new Analyzer() {

            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer t = new StandardTokenizer();
                return new TokenStreamComponents(t, new LowerCaseFilter(t));
            }

        });
        PerFieldAnalyzerWrapper wrapper = new PerFieldAnalyzerWrapper(new WhitespaceAnalyzer(), mapping);

        IndexWriterConfig conf = new IndexWriterConfig(wrapper);
        IndexWriter writer = new IndexWriter(dir, conf);
        String[] strings = new String[]{
            "Xorr the God-Jewel",
            "Grog the God-Crusher",
            "Xorn",
            "Walter Newell",
            "Wanda Maximoff",
            "Captain America",
            "American Ace",
            "USA Hero",
            "Wundarr the Aquarian",
            "Will o' the Wisp",
            "Xemnu the Titan",
            "Fantastic Four",
            "Quasar",
            "Quasar II"
        };
        for (String line : strings) {
            Document doc = new Document();
            doc.add(new Field("body", line, TextField.TYPE_NOT_STORED));
            doc.add(new Field("body_ngram", line, TextField.TYPE_NOT_STORED));
            writer.addDocument(doc);
        }

        DirectoryReader ir = DirectoryReader.open(writer);
        WordScorer wordScorer = new LaplaceScorer(ir, MultiTerms.getTerms(ir, "body_ngram"), "body_ngram", 0.95d,
            new BytesRef(" "), 0.5f);

        NoisyChannelSpellChecker suggester = new NoisyChannelSpellChecker(REAL_WORD_LIKELIHOOD, true, DEFAULT_TOKEN_LIMIT);
        DirectSpellChecker spellchecker = new DirectSpellChecker();
        spellchecker.setMinQueryLength(1);
        DirectCandidateGenerator generator = new DirectCandidateGenerator(spellchecker, "body", SuggestMode.SUGGEST_MORE_POPULAR,
            ir, 0.95, 5);
        Result result = getCorrections(suggester, wrapper, new BytesRef("american ame"), generator, 1, 1,
            ir, "body", wordScorer, 1, 2);
        Correction[] corrections = result.corrections;
        assertThat(corrections.length, equalTo(1));
        assertThat(corrections[0].join(space).utf8ToString(), equalTo("american ace"));
        assertThat(corrections[0].join(space, preTag, postTag).utf8ToString(), equalTo("american <em>ace</em>"));
        assertThat(result.cutoffScore, greaterThan(0d));

        result = getCorrections(suggester, wrapper, new BytesRef("american ame"), generator, 1, 1,
            ir, "body", wordScorer, 0, 1);
        corrections = result.corrections;
        assertThat(corrections.length, equalTo(1));
        assertThat(corrections[0].join(space).utf8ToString(), equalTo("american ame"));
        assertThat(corrections[0].join(space, preTag, postTag).utf8ToString(), equalTo("american ame"));
        assertThat(result.cutoffScore, equalTo(Double.MIN_VALUE));

        suggester = new NoisyChannelSpellChecker(0.85, true, DEFAULT_TOKEN_LIMIT);
        wordScorer = new LaplaceScorer(ir, MultiTerms.getTerms(ir, "body_ngram"), "body_ngram", 0.85d,
            new BytesRef(" "), 0.5f);
        corrections = getCorrections(suggester, wrapper, new BytesRef("Xor the Got-Jewel"), generator, 0.5f, 4,
            ir, "body", wordScorer, 0, 2).corrections;
        assertThat(corrections.length, equalTo(4));
        assertThat(corrections[0].join(space).utf8ToString(), equalTo("xorr the god jewel"));
        assertThat(corrections[1].join(space).utf8ToString(), equalTo("xor the god jewel"));
        assertThat(corrections[2].join(space).utf8ToString(), equalTo("xorn the god jewel"));
        assertThat(corrections[3].join(space).utf8ToString(), equalTo("xorr the got jewel"));
        assertThat(corrections[0].join(space, preTag, postTag).utf8ToString(), equalTo("<em>xorr</em> the <em>god</em> jewel"));
        assertThat(corrections[1].join(space, preTag, postTag).utf8ToString(), equalTo("xor the <em>god</em> jewel"));
        assertThat(corrections[2].join(space, preTag, postTag).utf8ToString(), equalTo("<em>xorn</em> the <em>god</em> jewel"));
        assertThat(corrections[3].join(space, preTag, postTag).utf8ToString(), equalTo("<em>xorr</em> the got jewel"));

        corrections = getCorrections(suggester, wrapper, new BytesRef("Xor the Got-Jewel"), generator, 0.5f,
            4, ir, "body", wordScorer, 1, 2).corrections;
        assertThat(corrections.length, equalTo(4));
        assertThat(corrections[0].join(space).utf8ToString(), equalTo("xorr the god jewel"));
        assertThat(corrections[1].join(space).utf8ToString(), equalTo("xor the god jewel"));
        assertThat(corrections[2].join(space).utf8ToString(), equalTo("xorn the god jewel"));
        assertThat(corrections[3].join(space).utf8ToString(), equalTo("xorr the got jewel"));

        // Test some of the highlighting corner cases
        suggester = new NoisyChannelSpellChecker(0.85, true, DEFAULT_TOKEN_LIMIT);
        wordScorer = new LaplaceScorer(ir, MultiTerms.getTerms(ir, "body_ngram"), "body_ngram", 0.85d,
            new BytesRef(" "), 0.5f);
        corrections = getCorrections(suggester, wrapper, new BytesRef("Xor teh Got-Jewel"), generator, 4f, 4,
            ir, "body", wordScorer, 1, 2).corrections;
        assertThat(corrections.length, equalTo(4));
        assertThat(corrections[0].join(space).utf8ToString(), equalTo("xorr the god jewel"));
        assertThat(corrections[1].join(space).utf8ToString(), equalTo("xor the god jewel"));
        assertThat(corrections[2].join(space).utf8ToString(), equalTo("xorn the god jewel"));
        assertThat(corrections[3].join(space).utf8ToString(), equalTo("xor teh god jewel"));
        assertThat(corrections[0].join(space, preTag, postTag).utf8ToString(), equalTo("<em>xorr the god</em> jewel"));
        assertThat(corrections[1].join(space, preTag, postTag).utf8ToString(), equalTo("xor <em>the god</em> jewel"));
        assertThat(corrections[2].join(space, preTag, postTag).utf8ToString(), equalTo("<em>xorn the god</em> jewel"));
        assertThat(corrections[3].join(space, preTag, postTag).utf8ToString(), equalTo("xor teh <em>god</em> jewel"));

        // test synonyms

        Analyzer analyzer = new Analyzer() {

            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer t = new StandardTokenizer();
                TokenFilter filter = new LowerCaseFilter(t);
                try {
                    SolrSynonymParser parser = new SolrSynonymParser(true, false, new WhitespaceAnalyzer());
                    parser.parse(new StringReader("usa => usa, america, american"));
                    filter = new SynonymFilter(filter, parser.build(), true);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return new TokenStreamComponents(t, filter);
            }
        };

        spellchecker.setAccuracy(0.0f);
        spellchecker.setMinPrefix(1);
        spellchecker.setMinQueryLength(1);
        suggester = new NoisyChannelSpellChecker(0.85, true, DEFAULT_TOKEN_LIMIT);
        wordScorer = new LaplaceScorer(ir, MultiTerms.getTerms(ir, "body_ngram"), "body_ngram", 0.85d,
            new BytesRef(" "), 0.5f);
        corrections = getCorrections(suggester, analyzer, new BytesRef("captian usa"), generator, 2, 4,
            ir, "body", wordScorer, 1, 2).corrections;
        assertThat(corrections[0].join(space).utf8ToString(), equalTo("captain america"));
        assertThat(corrections[0].join(space, preTag, postTag).utf8ToString(), equalTo("<em>captain america</em>"));

        generator = new DirectCandidateGenerator(spellchecker, "body", SuggestMode.SUGGEST_MORE_POPULAR, ir, 0.85,
            10, null, analyzer, MultiTerms.getTerms(ir, "body"));
        corrections = getCorrections(suggester, analyzer, new BytesRef("captian usw"), generator, 2, 4,
            ir, "body", wordScorer, 1, 2).corrections;
        assertThat(corrections[0].join(new BytesRef(" ")).utf8ToString(), equalTo("captain america"));
        assertThat(corrections[0].join(space, preTag, postTag).utf8ToString(), equalTo("<em>captain america</em>"));

        // Make sure that user supplied text is not marked as highlighted in the presence of a synonym filter
        generator = new DirectCandidateGenerator(spellchecker, "body", SuggestMode.SUGGEST_MORE_POPULAR, ir, 0.85,
            10, null, analyzer, MultiTerms.getTerms(ir, "body"));
        corrections = getCorrections(suggester, analyzer, new BytesRef("captain usw"), generator, 2, 4, ir,
            "body", wordScorer, 1, 2).corrections;
        assertThat(corrections[0].join(new BytesRef(" ")).utf8ToString(), equalTo("captain america"));
        assertThat(corrections[0].join(space, preTag, postTag).utf8ToString(), equalTo("captain <em>america</em>"));
    }

    public void testMultiGenerator() throws IOException {
        RAMDirectory dir = new RAMDirectory();
        Map<String, Analyzer> mapping = new HashMap<>();
        mapping.put("body_ngram", new Analyzer() {

            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer t = new StandardTokenizer();
                ShingleFilter tf = new ShingleFilter(t, 2, 3);
                tf.setOutputUnigrams(false);
                return new TokenStreamComponents(t, new LowerCaseFilter(tf));
            }

        });

        mapping.put("body", new Analyzer() {

            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer t = new StandardTokenizer();
                return new TokenStreamComponents(t, new LowerCaseFilter(t));
            }

        });
        mapping.put("body_reverse", new Analyzer() {

            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer t = new StandardTokenizer();
                return new TokenStreamComponents(t, new ReverseStringFilter(new LowerCaseFilter(t)));
            }

        });
        PerFieldAnalyzerWrapper wrapper = new PerFieldAnalyzerWrapper(new WhitespaceAnalyzer(), mapping);

        IndexWriterConfig conf = new IndexWriterConfig(wrapper);
        IndexWriter writer = new IndexWriter(dir, conf);
        String[] strings = new String[]{
            "Xorr the God-Jewel",
            "Grog the God-Crusher",
            "Xorn",
            "Walter Newell",
            "Wanda Maximoff",
            "Captain America",
            "American Ace",
            "Wundarr the Aquarian",
            "Will o' the Wisp",
            "Xemnu the Titan",
            "Fantastic Four",
            "Quasar",
            "Quasar II"
        };
        for (String line : strings) {
            Document doc = new Document();
            doc.add(new Field("body", line, TextField.TYPE_NOT_STORED));
            doc.add(new Field("body_reverse", line, TextField.TYPE_NOT_STORED));
            doc.add(new Field("body_ngram", line, TextField.TYPE_NOT_STORED));
            writer.addDocument(doc);
        }

        DirectoryReader ir = DirectoryReader.open(writer);
        LaplaceScorer wordScorer = new LaplaceScorer(ir, MultiTerms.getTerms(ir, "body_ngram"), "body_ngram", 0.95d,
            new BytesRef(" "), 0.5f);
        NoisyChannelSpellChecker suggester = new NoisyChannelSpellChecker(REAL_WORD_LIKELIHOOD, true, DEFAULT_TOKEN_LIMIT);
        DirectSpellChecker spellchecker = new DirectSpellChecker();
        spellchecker.setMinQueryLength(1);
        DirectCandidateGenerator forward = new DirectCandidateGenerator(spellchecker, "body", SuggestMode.SUGGEST_ALWAYS, ir,
            0.95, 10);
        DirectCandidateGenerator reverse = new DirectCandidateGenerator(spellchecker, "body_reverse", SuggestMode.SUGGEST_ALWAYS, ir,
            0.95, 10, wrapper, wrapper,  MultiTerms.getTerms(ir, "body_reverse"));
        CandidateGenerator generator = new MultiCandidateGeneratorWrapper(10, forward, reverse);

        Correction[] corrections = getCorrections(suggester, wrapper, new BytesRef("american cae"), generator, 1, 1,
            ir, "body", wordScorer, 1, 2).corrections;
        assertThat(corrections.length, equalTo(1));
        assertThat(corrections[0].join(new BytesRef(" ")).utf8ToString(), equalTo("american ace"));

        generator = new MultiCandidateGeneratorWrapper(5, forward, reverse);
        corrections = getCorrections(suggester, wrapper, new BytesRef("american ame"), generator, 1, 1, ir,
            "body", wordScorer, 1, 2).corrections;
        assertThat(corrections.length, equalTo(1));
        assertThat(corrections[0].join(new BytesRef(" ")).utf8ToString(), equalTo("american ace"));

        corrections = getCorrections(suggester, wrapper, new BytesRef("american cae"), forward, 1, 1, ir,
            "body", wordScorer, 1, 2).corrections;
        assertThat(corrections.length, equalTo(0)); // only use forward with constant prefix

        corrections = getCorrections(suggester, wrapper, new BytesRef("america cae"), generator, 2, 1, ir,
            "body", wordScorer, 1, 2).corrections;
        assertThat(corrections.length, equalTo(1));
        assertThat(corrections[0].join(new BytesRef(" ")).utf8ToString(), equalTo("american ace"));

        corrections = getCorrections(suggester, wrapper, new BytesRef("Zorr the Got-Jewel"), generator, 0.5f, 4, ir,
            "body", wordScorer, 0, 2).corrections;
        assertThat(corrections.length, equalTo(4));
        assertThat(corrections[0].join(new BytesRef(" ")).utf8ToString(), equalTo("xorr the god jewel"));
        assertThat(corrections[1].join(new BytesRef(" ")).utf8ToString(), equalTo("zorr the god jewel"));
        assertThat(corrections[2].join(new BytesRef(" ")).utf8ToString(), equalTo("four the god jewel"));


        corrections = getCorrections(suggester, wrapper, new BytesRef("Zorr the Got-Jewel"), generator, 0.5f, 1, ir,
            "body", wordScorer, 1.5f, 2).corrections;
        assertThat(corrections.length, equalTo(1));
        assertThat(corrections[0].join(new BytesRef(" ")).utf8ToString(), equalTo("xorr the god jewel"));

        corrections = getCorrections(suggester, wrapper, new BytesRef("Xor the Got-Jewel"), generator, 0.5f, 1, ir,
            "body", wordScorer, 1.5f, 2).corrections;
        assertThat(corrections.length, equalTo(1));
        assertThat(corrections[0].join(new BytesRef(" ")).utf8ToString(), equalTo("xorr the god jewel"));

        // Test a special case where one of the suggest term is unchanged by the postFilter, 'II' here is unchanged by the reverse analyzer.
        corrections = getCorrections(suggester, wrapper, new BytesRef("Quazar II"), generator, 1, 1, ir,
            "body", wordScorer, 1, 2).corrections;
        assertThat(corrections.length, equalTo(1));
        assertThat(corrections[0].join(new BytesRef(" ")).utf8ToString(), equalTo("quasar ii"));
    }

    public void testTrigram() throws IOException {
        RAMDirectory dir = new RAMDirectory();
        Map<String, Analyzer> mapping = new HashMap<>();
        mapping.put("body_ngram", new Analyzer() {

            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer t = new StandardTokenizer();
                ShingleFilter tf = new ShingleFilter(t, 2, 3);
                tf.setOutputUnigrams(false);
                return new TokenStreamComponents(t, new LowerCaseFilter(tf));
            }

        });

        mapping.put("body", new Analyzer() {

            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer t = new StandardTokenizer();
                return new TokenStreamComponents(t, new LowerCaseFilter(t));
            }

        });
        PerFieldAnalyzerWrapper wrapper = new PerFieldAnalyzerWrapper(new WhitespaceAnalyzer(), mapping);

        IndexWriterConfig conf = new IndexWriterConfig(wrapper);
        IndexWriter writer = new IndexWriter(dir, conf);
        String[] strings = new String[]{
            "Xorr the God-Jewel",
            "Grog the God-Crusher",
            "Xorn",
            "Walter Newell",
            "Wanda Maximoff",
            "Captain America",
            "American Ace",
            "USA Hero",
            "Wundarr the Aquarian",
            "Will o' the Wisp",
            "Xemnu the Titan",
            "Fantastic Four",
            "Quasar",
            "Quasar II"
        };
        for (String line : strings) {
            Document doc = new Document();
            doc.add(new Field("body", line, TextField.TYPE_NOT_STORED));
            doc.add(new Field("body_ngram", line, TextField.TYPE_NOT_STORED));
            writer.addDocument(doc);
        }

        DirectoryReader ir = DirectoryReader.open(writer);
        WordScorer wordScorer = new LinearInterpolatingScorer(ir, MultiTerms.getTerms(ir, "body_ngram"), "body_ngram", 0.85d,
            new BytesRef(" "), 0.5, 0.4, 0.1);

        NoisyChannelSpellChecker suggester = new NoisyChannelSpellChecker(REAL_WORD_LIKELIHOOD, true, DEFAULT_TOKEN_LIMIT);
        DirectSpellChecker spellchecker = new DirectSpellChecker();
        spellchecker.setMinQueryLength(1);
        DirectCandidateGenerator generator = new DirectCandidateGenerator(spellchecker, "body", SuggestMode.SUGGEST_MORE_POPULAR, ir,
            0.95, 5);
        Correction[] corrections = getCorrections(suggester, wrapper, new BytesRef("american ame"), generator, 1, 1,
            ir, "body", wordScorer, 1, 3).corrections;
        assertThat(corrections.length, equalTo(1));
        assertThat(corrections[0].join(new BytesRef(" ")).utf8ToString(), equalTo("american ace"));

        corrections = getCorrections(suggester, wrapper, new BytesRef("american ame"), generator, 1, 1,
            ir, "body", wordScorer, 1, 1).corrections;
        assertThat(corrections.length, equalTo(0));
//        assertThat(corrections[0].join(new BytesRef(" ")).utf8ToString(), equalTo("american ape"));

        wordScorer = new LinearInterpolatingScorer(ir, MultiTerms.getTerms(ir, "body_ngram"), "body_ngram", 0.85d,
            new BytesRef(" "), 0.5, 0.4, 0.1);
        corrections = getCorrections(suggester, wrapper, new BytesRef("Xor the Got-Jewel"), generator, 0.5f, 4,
            ir, "body", wordScorer, 0, 3).corrections;
        assertThat(corrections.length, equalTo(4));
        assertThat(corrections[0].join(new BytesRef(" ")).utf8ToString(), equalTo("xorr the god jewel"));
        assertThat(corrections[1].join(new BytesRef(" ")).utf8ToString(), equalTo("xor the god jewel"));
        assertThat(corrections[2].join(new BytesRef(" ")).utf8ToString(), equalTo("xorn the god jewel"));
        assertThat(corrections[3].join(new BytesRef(" ")).utf8ToString(), equalTo("xorr the got jewel"));




        corrections = getCorrections(suggester, wrapper, new BytesRef("Xor the Got-Jewel"), generator, 0.5f, 4,
            ir, "body", wordScorer, 1, 3).corrections;
        assertThat(corrections.length, equalTo(4));
        assertThat(corrections[0].join(new BytesRef(" ")).utf8ToString(), equalTo("xorr the god jewel"));
        assertThat(corrections[1].join(new BytesRef(" ")).utf8ToString(), equalTo("xor the god jewel"));
        assertThat(corrections[2].join(new BytesRef(" ")).utf8ToString(), equalTo("xorn the god jewel"));
        assertThat(corrections[3].join(new BytesRef(" ")).utf8ToString(), equalTo("xorr the got jewel"));


        corrections = getCorrections(suggester, wrapper, new BytesRef("Xor the Got-Jewel"), generator, 0.5f, 1,
            ir, "body", wordScorer, 100, 3).corrections;
        assertThat(corrections.length, equalTo(1));
        assertThat(corrections[0].join(new BytesRef(" ")).utf8ToString(), equalTo("xorr the god jewel"));


        // test synonyms

        Analyzer analyzer = new Analyzer() {

            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer t = new StandardTokenizer();
                TokenFilter filter = new LowerCaseFilter(t);
                try {
                    SolrSynonymParser parser = new SolrSynonymParser(true, false, new WhitespaceAnalyzer());
                    parser.parse(new StringReader("usa => usa, america, american"));
                    filter = new SynonymFilter(filter, parser.build(), true);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return new TokenStreamComponents(t, filter);
            }
        };

        spellchecker.setAccuracy(0.0f);
        spellchecker.setMinPrefix(1);
        spellchecker.setMinQueryLength(1);
        suggester = new NoisyChannelSpellChecker(0.95, true, DEFAULT_TOKEN_LIMIT);
        wordScorer = new LinearInterpolatingScorer(ir, MultiTerms.getTerms(ir, "body_ngram"), "body_ngram", 0.95d,
            new BytesRef(" "),  0.5, 0.4, 0.1);
        corrections = getCorrections(suggester, analyzer, new BytesRef("captian usa"), generator, 2, 4,
            ir, "body", wordScorer, 1, 3).corrections;
        assertThat(corrections[0].join(new BytesRef(" ")).utf8ToString(), equalTo("captain america"));

        generator = new DirectCandidateGenerator(spellchecker, "body", SuggestMode.SUGGEST_MORE_POPULAR, ir, 0.95,
            10, null, analyzer, MultiTerms.getTerms(ir, "body"));
        corrections = getCorrections(suggester, analyzer, new BytesRef("captian usw"), generator, 2, 4,
            ir, "body", wordScorer, 1, 3).corrections;
        assertThat(corrections[0].join(new BytesRef(" ")).utf8ToString(), equalTo("captain america"));


        wordScorer = new StupidBackoffScorer(ir, MultiTerms.getTerms(ir, "body_ngram"), "body_ngram", 0.85d,
            new BytesRef(" "), 0.4);
        corrections = getCorrections(suggester, wrapper, new BytesRef("Xor the Got-Jewel"), generator, 0.5f, 2,
            ir, "body", wordScorer, 0, 3).corrections;
        assertThat(corrections.length, equalTo(2));
        assertThat(corrections[0].join(new BytesRef(" ")).utf8ToString(), equalTo("xorr the god jewel"));
        assertThat(corrections[1].join(new BytesRef(" ")).utf8ToString(), equalTo("xor the god jewel"));
    }

    public void testFewDocsEgdeCase() throws Exception {
        try (Directory dir = newDirectory()) {
            try (IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig())) {
                Document document = new Document();
                document.add(new TextField("field", "value", Field.Store.NO));
                iw.addDocument(document);
                iw.commit();
                document = new Document();
                document.add(new TextField("other_field", "value", Field.Store.NO));
                iw.addDocument(document);
            }

            try (DirectoryReader ir = DirectoryReader.open(dir)) {
                WordScorer wordScorer = new StupidBackoffScorer(ir, MultiTerms.getTerms(ir, "field"), "field",  0.95d,
                    new BytesRef(" "), 0.4f);
                NoisyChannelSpellChecker suggester = new NoisyChannelSpellChecker(REAL_WORD_LIKELIHOOD, true, DEFAULT_TOKEN_LIMIT);
                DirectSpellChecker spellchecker = new DirectSpellChecker();
                DirectCandidateGenerator generator = new DirectCandidateGenerator(spellchecker, "field",
                    SuggestMode.SUGGEST_MORE_POPULAR, ir, 0.95, 5);
                Result result = getCorrections(suggester, new StandardAnalyzer(), new BytesRef("valeu"), generator, 1, 1,
                    ir, "field", wordScorer, 1, 2);
                assertThat(result.corrections.length, equalTo(1));
                assertThat(result.corrections[0].join(space).utf8ToString(), equalTo("value"));
            }
        }
    }

    private Result getCorrections(NoisyChannelSpellChecker checker, Analyzer analyzer, BytesRef query, CandidateGenerator generator,
            float maxErrors, int numCorrections, IndexReader reader, String analysisField, WordScorer scorer, float confidence,
            int gramSize) throws IOException {
        CharsRefBuilder spare = new CharsRefBuilder();
        spare.copyUTF8Bytes(query);
        TokenStream tokenStream = analyzer.tokenStream(analysisField, new CharArrayReader(spare.chars(), 0, spare.length()));
        return checker.getCorrections(tokenStream, generator, maxErrors, numCorrections, scorer, confidence, gramSize);
    }

}
