package org.elasticsearch.test.unit.search.suggest.phrase;
/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import com.google.common.base.Charsets;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.reverse.ReverseStringFilter;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.synonym.SolrSynonymParser;
import org.apache.lucene.analysis.synonym.SynonymFilter;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.spell.DirectSpellChecker;
import org.apache.lucene.search.spell.SuggestMode;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.elasticsearch.search.suggest.phrase.*;
import org.elasticsearch.test.integration.ElasticsearchTestCase;
import org.junit.Test;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
public class NoisyChannelSpellCheckerTests extends ElasticsearchTestCase{

    @Test
    public void testMarvelHeros() throws IOException {
        RAMDirectory dir = new RAMDirectory();
        Map<String, Analyzer> mapping = new HashMap<String, Analyzer>();
        mapping.put("body_ngram", new Analyzer() {

            @Override
            protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
                Tokenizer t = new StandardTokenizer(Version.LUCENE_41, reader);
                ShingleFilter tf = new ShingleFilter(t, 2, 3);
                tf.setOutputUnigrams(false);
                return new TokenStreamComponents(t, new LowerCaseFilter(Version.LUCENE_41, tf));
            }

        });

        mapping.put("body", new Analyzer() {

            @Override
            protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
                Tokenizer t = new StandardTokenizer(Version.LUCENE_41, reader);
                return new TokenStreamComponents(t, new LowerCaseFilter(Version.LUCENE_41, t));
            }

        });
        PerFieldAnalyzerWrapper wrapper = new PerFieldAnalyzerWrapper(new WhitespaceAnalyzer(Version.LUCENE_41), mapping);

        IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_41, wrapper);
        IndexWriter writer = new IndexWriter(dir, conf);
        BufferedReader reader = new BufferedReader(new InputStreamReader(NoisyChannelSpellCheckerTests.class.getResourceAsStream("/config/names.txt"), Charsets.UTF_8));
        String line = null;
        while ((line = reader.readLine()) != null) {
            Document doc = new Document();
            doc.add(new Field("body", line, TextField.TYPE_NOT_STORED));
            doc.add(new Field("body_ngram", line, TextField.TYPE_NOT_STORED));
            writer.addDocument(doc);
        }

        DirectoryReader ir = DirectoryReader.open(writer, false);
        WordScorer wordScorer = new LaplaceScorer(ir, "body_ngram", 0.95d, new BytesRef(" "), 0.5f);
        
        NoisyChannelSpellChecker suggester = new NoisyChannelSpellChecker();
        DirectSpellChecker spellchecker = new DirectSpellChecker();
        spellchecker.setMinQueryLength(1);
        DirectCandidateGenerator generator = new DirectCandidateGenerator(spellchecker, "body", SuggestMode.SUGGEST_MORE_POPULAR, ir, 0.95, 5);
        Correction[] corrections = suggester.getCorrections(wrapper, new BytesRef("american ame"), generator, 1, 1, ir, "body", wordScorer, 1, 2);
        assertThat(corrections.length, equalTo(1));
        assertThat(corrections[0].join(new BytesRef(" ")).utf8ToString(), equalTo("american ace"));
        
        corrections = suggester.getCorrections(wrapper, new BytesRef("american ame"), generator, 1, 1, ir, "body", wordScorer, 0, 1);
        assertThat(corrections.length, equalTo(1));
        assertThat(corrections[0].join(new BytesRef(" ")).utf8ToString(), equalTo("american ame"));
        
        suggester = new NoisyChannelSpellChecker(0.85);
        wordScorer = new LaplaceScorer(ir, "body_ngram", 0.85d, new BytesRef(" "), 0.5f);
        corrections = suggester.getCorrections(wrapper, new BytesRef("Xor the Got-Jewel"), generator, 0.5f, 4, ir, "body", wordScorer, 0, 2);
        assertThat(corrections.length, equalTo(4));
        assertThat(corrections[0].join(new BytesRef(" ")).utf8ToString(), equalTo("xorr the god jewel"));
        assertThat(corrections[1].join(new BytesRef(" ")).utf8ToString(), equalTo("xor the god jewel"));
        assertThat(corrections[2].join(new BytesRef(" ")).utf8ToString(), equalTo("xorn the god jewel"));
        assertThat(corrections[3].join(new BytesRef(" ")).utf8ToString(), equalTo("xorr the got jewel"));
        
        corrections = suggester.getCorrections(wrapper, new BytesRef("Xor the Got-Jewel"), generator, 0.5f, 4, ir, "body", wordScorer, 1, 2);
        assertThat(corrections.length, equalTo(4));
        assertThat(corrections[0].join(new BytesRef(" ")).utf8ToString(), equalTo("xorr the god jewel"));
        assertThat(corrections[1].join(new BytesRef(" ")).utf8ToString(), equalTo("xor the god jewel"));
        assertThat(corrections[2].join(new BytesRef(" ")).utf8ToString(), equalTo("xorn the god jewel"));
        assertThat(corrections[3].join(new BytesRef(" ")).utf8ToString(), equalTo("xorr the got jewel"));
        

        // test synonyms
        
        Analyzer analyzer = new Analyzer() {

            @Override
            protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
                Tokenizer t = new StandardTokenizer(Version.LUCENE_41, reader);
                TokenFilter filter = new LowerCaseFilter(Version.LUCENE_41, t);
                try {
                    SolrSynonymParser parser = new SolrSynonymParser(true, false, new WhitespaceAnalyzer(Version.LUCENE_41));
                    ((SolrSynonymParser) parser).add(new StringReader("usa => usa, america, american\nursa => usa, america, american"));
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
        suggester = new NoisyChannelSpellChecker(0.85);
        wordScorer = new LaplaceScorer(ir, "body_ngram", 0.85d, new BytesRef(" "), 0.5f);
        corrections = suggester.getCorrections(analyzer, new BytesRef("captian usa"), generator, 2, 4, ir, "body", wordScorer, 1, 2);
        assertThat(corrections[0].join(new BytesRef(" ")).utf8ToString(), equalTo("captain america"));
        
        generator = new DirectCandidateGenerator(spellchecker, "body", SuggestMode.SUGGEST_MORE_POPULAR, ir, 0.85, 10, null, analyzer);
        corrections = suggester.getCorrections(analyzer, new BytesRef("captian usw"), generator, 2, 4, ir, "body", wordScorer, 1, 2);
        assertThat(corrections[0].join(new BytesRef(" ")).utf8ToString(), equalTo("captain america"));
    }
    
    @Test
    public void testMarvelHerosMultiGenerator() throws IOException {
        RAMDirectory dir = new RAMDirectory();
        Map<String, Analyzer> mapping = new HashMap<String, Analyzer>();
        mapping.put("body_ngram", new Analyzer() {

            @Override
            protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
                Tokenizer t = new StandardTokenizer(Version.LUCENE_41, reader);
                ShingleFilter tf = new ShingleFilter(t, 2, 3);
                tf.setOutputUnigrams(false);
                return new TokenStreamComponents(t, new LowerCaseFilter(Version.LUCENE_41, tf));
            }

        });

        mapping.put("body", new Analyzer() {

            @Override
            protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
                Tokenizer t = new StandardTokenizer(Version.LUCENE_41, reader);
                return new TokenStreamComponents(t, new LowerCaseFilter(Version.LUCENE_41, t));
            }

        });
        mapping.put("body_reverse", new Analyzer() {

            @Override
            protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
                Tokenizer t = new StandardTokenizer(Version.LUCENE_41, reader);
                return new TokenStreamComponents(t, new ReverseStringFilter(Version.LUCENE_41, new LowerCaseFilter(Version.LUCENE_41, t)));
            }

        });
        PerFieldAnalyzerWrapper wrapper = new PerFieldAnalyzerWrapper(new WhitespaceAnalyzer(Version.LUCENE_41), mapping);

        IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_41, wrapper);
        IndexWriter writer = new IndexWriter(dir, conf);
        BufferedReader reader = new BufferedReader(new InputStreamReader(NoisyChannelSpellCheckerTests.class.getResourceAsStream("/config/names.txt"), Charsets.UTF_8));
        String line = null;
        while ((line = reader.readLine()) != null) {
            Document doc = new Document();
            doc.add(new Field("body", line, TextField.TYPE_NOT_STORED));
            doc.add(new Field("body_reverse", line, TextField.TYPE_NOT_STORED));
            doc.add(new Field("body_ngram", line, TextField.TYPE_NOT_STORED));
            writer.addDocument(doc);
        }

        DirectoryReader ir = DirectoryReader.open(writer, false);
        LaplaceScorer wordScorer = new LaplaceScorer(ir, "body_ngram", 0.95d, new BytesRef(" "), 0.5f);
        NoisyChannelSpellChecker suggester = new NoisyChannelSpellChecker();
        DirectSpellChecker spellchecker = new DirectSpellChecker();
        spellchecker.setMinQueryLength(1);
        DirectCandidateGenerator forward = new DirectCandidateGenerator(spellchecker, "body", SuggestMode.SUGGEST_ALWAYS, ir, 0.95, 10);
        DirectCandidateGenerator reverse = new DirectCandidateGenerator(spellchecker, "body_reverse", SuggestMode.SUGGEST_ALWAYS, ir, 0.95, 10, wrapper, wrapper);
        CandidateGenerator generator = new MultiCandidateGeneratorWrapper(10, forward, reverse);
        
        Correction[] corrections = suggester.getCorrections(wrapper, new BytesRef("american cae"), generator, 1, 1, ir, "body", wordScorer, 1, 2);
        assertThat(corrections.length, equalTo(1));
        assertThat(corrections[0].join(new BytesRef(" ")).utf8ToString(), equalTo("american ace"));
        
        generator = new MultiCandidateGeneratorWrapper(5, forward, reverse);
        corrections = suggester.getCorrections(wrapper, new BytesRef("american ame"), generator, 1, 1, ir, "body", wordScorer, 1, 2);
        assertThat(corrections.length, equalTo(1));
        assertThat(corrections[0].join(new BytesRef(" ")).utf8ToString(), equalTo("american ace"));
        
        corrections = suggester.getCorrections(wrapper, new BytesRef("american cae"), forward, 1, 1, ir, "body", wordScorer, 1, 2);
        assertThat(corrections.length, equalTo(0)); // only use forward with constant prefix
        
        corrections = suggester.getCorrections(wrapper, new BytesRef("america cae"), generator, 2, 1, ir, "body", wordScorer, 1, 2);
        assertThat(corrections.length, equalTo(1));
        assertThat(corrections[0].join(new BytesRef(" ")).utf8ToString(), equalTo("american ace"));
        
        corrections = suggester.getCorrections(wrapper, new BytesRef("Zorr the Got-Jewel"), generator, 0.5f, 4, ir, "body", wordScorer, 0, 2);
        assertThat(corrections.length, equalTo(4));
        assertThat(corrections[0].join(new BytesRef(" ")).utf8ToString(), equalTo("xorr the god jewel"));
        assertThat(corrections[1].join(new BytesRef(" ")).utf8ToString(), equalTo("zorr the god jewel"));
        assertThat(corrections[2].join(new BytesRef(" ")).utf8ToString(), equalTo("gorr the god jewel"));
        assertThat(corrections[3].join(new BytesRef(" ")).utf8ToString(), equalTo("tarr the god jewel"));
        
        

        corrections = suggester.getCorrections(wrapper, new BytesRef("Zorr the Got-Jewel"), generator, 0.5f, 1, ir, "body", wordScorer, 1.5f, 2);
        assertThat(corrections.length, equalTo(1));
        assertThat(corrections[0].join(new BytesRef(" ")).utf8ToString(), equalTo("xorr the god jewel"));
        
        corrections = suggester.getCorrections(wrapper, new BytesRef("Xor the Got-Jewel"), generator, 0.5f, 1, ir, "body", wordScorer, 1.5f, 2);
        assertThat(corrections.length, equalTo(1));
        assertThat(corrections[0].join(new BytesRef(" ")).utf8ToString(), equalTo("xorr the god jewel"));

    }
    
    @Test
    public void testMarvelHerosTrigram() throws IOException {
        
      
        RAMDirectory dir = new RAMDirectory();
        Map<String, Analyzer> mapping = new HashMap<String, Analyzer>();
        mapping.put("body_ngram", new Analyzer() {

            @Override
            protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
                Tokenizer t = new StandardTokenizer(Version.LUCENE_41, reader);
                ShingleFilter tf = new ShingleFilter(t, 2, 3);
                tf.setOutputUnigrams(false);
                return new TokenStreamComponents(t, new LowerCaseFilter(Version.LUCENE_41, tf));
            }

        });

        mapping.put("body", new Analyzer() {

            @Override
            protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
                Tokenizer t = new StandardTokenizer(Version.LUCENE_41, reader);
                return new TokenStreamComponents(t, new LowerCaseFilter(Version.LUCENE_41, t));
            }

        });
        PerFieldAnalyzerWrapper wrapper = new PerFieldAnalyzerWrapper(new WhitespaceAnalyzer(Version.LUCENE_41), mapping);

        IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_41, wrapper);
        IndexWriter writer = new IndexWriter(dir, conf);
        BufferedReader reader = new BufferedReader(new InputStreamReader(NoisyChannelSpellCheckerTests.class.getResourceAsStream("/config/names.txt"), Charsets.UTF_8));
        String line = null;
        while ((line = reader.readLine()) != null) {
            Document doc = new Document();
            doc.add(new Field("body", line, TextField.TYPE_NOT_STORED));
            doc.add(new Field("body_ngram", line, TextField.TYPE_NOT_STORED));
            writer.addDocument(doc);
        }

        DirectoryReader ir = DirectoryReader.open(writer, false);
        WordScorer wordScorer = new LinearInterpoatingScorer(ir, "body_ngram", 0.85d, new BytesRef(" "), 0.5, 0.4, 0.1);

        NoisyChannelSpellChecker suggester = new NoisyChannelSpellChecker();
        DirectSpellChecker spellchecker = new DirectSpellChecker();
        spellchecker.setMinQueryLength(1);
        DirectCandidateGenerator generator = new DirectCandidateGenerator(spellchecker, "body", SuggestMode.SUGGEST_MORE_POPULAR, ir, 0.95, 5);
        Correction[] corrections = suggester.getCorrections(wrapper, new BytesRef("american ame"), generator, 1, 1, ir, "body", wordScorer, 1, 3);
        assertThat(corrections.length, equalTo(1));
        assertThat(corrections[0].join(new BytesRef(" ")).utf8ToString(), equalTo("american ace"));
        
        corrections = suggester.getCorrections(wrapper, new BytesRef("american ame"), generator, 1, 1, ir, "body", wordScorer, 1, 1);
        assertThat(corrections.length, equalTo(0));
//        assertThat(corrections[0].join(new BytesRef(" ")).utf8ToString(), equalTo("american ape"));
        
        wordScorer = new LinearInterpoatingScorer(ir, "body_ngram", 0.85d, new BytesRef(" "), 0.5, 0.4, 0.1);
        corrections = suggester.getCorrections(wrapper, new BytesRef("Xor the Got-Jewel"), generator, 0.5f, 4, ir, "body", wordScorer, 0, 3);
        assertThat(corrections.length, equalTo(4));
        assertThat(corrections[0].join(new BytesRef(" ")).utf8ToString(), equalTo("xorr the god jewel"));
        assertThat(corrections[1].join(new BytesRef(" ")).utf8ToString(), equalTo("xor the god jewel"));
        assertThat(corrections[2].join(new BytesRef(" ")).utf8ToString(), equalTo("xorn the god jewel"));
        assertThat(corrections[3].join(new BytesRef(" ")).utf8ToString(), equalTo("xorr the got jewel"));
        
      

        
        corrections = suggester.getCorrections(wrapper, new BytesRef("Xor the Got-Jewel"), generator, 0.5f, 4, ir, "body", wordScorer, 1, 3);
        assertThat(corrections.length, equalTo(4));
        assertThat(corrections[0].join(new BytesRef(" ")).utf8ToString(), equalTo("xorr the god jewel"));
        assertThat(corrections[1].join(new BytesRef(" ")).utf8ToString(), equalTo("xor the god jewel"));
        assertThat(corrections[2].join(new BytesRef(" ")).utf8ToString(), equalTo("xorn the god jewel"));
        assertThat(corrections[3].join(new BytesRef(" ")).utf8ToString(), equalTo("xorr the got jewel"));
        

        corrections = suggester.getCorrections(wrapper, new BytesRef("Xor the Got-Jewel"), generator, 0.5f, 1, ir, "body", wordScorer, 100, 3);
        assertThat(corrections.length, equalTo(1));
        assertThat(corrections[0].join(new BytesRef(" ")).utf8ToString(), equalTo("xorr the god jewel"));
        

        // test synonyms
        
        Analyzer analyzer = new Analyzer() {

            @Override
            protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
                Tokenizer t = new StandardTokenizer(Version.LUCENE_41, reader);
                TokenFilter filter = new LowerCaseFilter(Version.LUCENE_41, t);
                try {
                    SolrSynonymParser parser = new SolrSynonymParser(true, false, new WhitespaceAnalyzer(Version.LUCENE_41));
                    ((SolrSynonymParser) parser).add(new StringReader("usa => usa, america, american\nursa => usa, america, american"));
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
        suggester = new NoisyChannelSpellChecker(0.95);
        wordScorer = new LinearInterpoatingScorer(ir, "body_ngram", 0.95d, new BytesRef(" "),  0.5, 0.4, 0.1);
        corrections = suggester.getCorrections(analyzer, new BytesRef("captian usa"), generator, 2, 4, ir, "body", wordScorer, 1, 3);
        assertThat(corrections[0].join(new BytesRef(" ")).utf8ToString(), equalTo("captain america"));
        
        generator = new DirectCandidateGenerator(spellchecker, "body", SuggestMode.SUGGEST_MORE_POPULAR, ir, 0.95, 10, null, analyzer);
        corrections = suggester.getCorrections(analyzer, new BytesRef("captian usw"), generator, 2, 4, ir, "body", wordScorer, 1, 3);
        assertThat(corrections[0].join(new BytesRef(" ")).utf8ToString(), equalTo("captain america"));
        
        
        wordScorer = new StupidBackoffScorer(ir, "body_ngram", 0.85d, new BytesRef(" "), 0.4);
        corrections = suggester.getCorrections(wrapper, new BytesRef("Xor the Got-Jewel"), generator, 0.5f, 2, ir, "body", wordScorer, 0, 3);
        assertThat(corrections.length, equalTo(2));
        assertThat(corrections[0].join(new BytesRef(" ")).utf8ToString(), equalTo("xorr the god jewel"));
        assertThat(corrections[1].join(new BytesRef(" ")).utf8ToString(), equalTo("xor the god jewel"));
    }
}
