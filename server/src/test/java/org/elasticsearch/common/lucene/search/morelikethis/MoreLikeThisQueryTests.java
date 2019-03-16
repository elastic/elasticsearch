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

package org.elasticsearch.common.lucene.search.morelikethis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TermFrequencyAttribute;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.MoreLikeThisQuery;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.function.BiFunction;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class MoreLikeThisQueryTests extends ESTestCase {
    public void testSimple() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));
        indexWriter.commit();

        Document document = new Document();
        document.add(new TextField("_id", "1", Field.Store.YES));
        document.add(new TextField("text", "lucene", Field.Store.YES));
        indexWriter.addDocument(document);

        document = new Document();
        document.add(new TextField("_id", "2", Field.Store.YES));
        document.add(new TextField("text", "lucene release", Field.Store.YES));
        indexWriter.addDocument(document);

        IndexReader reader = DirectoryReader.open(indexWriter);
        IndexSearcher searcher = new IndexSearcher(reader);

        MoreLikeThisQuery mltQuery = new MoreLikeThisQuery("lucene", new String[]{"text"}, Lucene.STANDARD_ANALYZER);
        mltQuery.setLikeText("lucene");
        mltQuery.setMinTermFrequency(1);
        mltQuery.setMinDocFreq(1);
        long count = searcher.count(mltQuery);
        assertThat(count, equalTo(2L));

        reader.close();
        indexWriter.close();
    }

    public void testCustomFrequency() throws Exception {
        Analyzer analyzer = new CustomFrequencyAnalyzer();

        FieldType fieldType = new FieldType();
        fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS); // cannot index positions while using custom TermFrequencyAttribute
        fieldType.setTokenized(true);
        fieldType.setStored(true);
        fieldType.freeze();

        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(analyzer));
        indexWriter.commit();

        Document document = new Document();
        document.add(new TextField("_id", "1", Field.Store.YES));
        document.add(new Field("text", "foo|1 bar|2", fieldType));
        indexWriter.addDocument(document);

        document = new Document();
        document.add(new TextField("_id", "2", Field.Store.YES));
        document.add(new Field("text", "foo|2 bar|1", fieldType));
        indexWriter.addDocument(document);

        IndexReader reader = DirectoryReader.open(indexWriter);
        IndexSearcher searcher = new IndexSearcher(reader);

        BiFunction<String, Integer, Query> makeQuery = (like, minTermFrequency) -> {
            MoreLikeThisQuery mltQuery = new MoreLikeThisQuery(like, new String[]{"text"}, analyzer);
            mltQuery.setMinTermFrequency(minTermFrequency);
            mltQuery.setMinDocFreq(1);
            mltQuery.setBoostTerms(true);
            return mltQuery;
        };

        // expect to get results as frequency of 'foo' >= 2
        TopDocs topDocs = searcher.search(makeQuery.apply("foo|2 bar|1", 2), 1);
        assertEquals(1, topDocs.scoreDocs.length);
        assertEquals(1L, topDocs.scoreDocs[0].doc);

        // Doc 1 should get higher score than doc 0 as 'foo' has higher
        // frequency in the 'like' text
        topDocs = searcher.search(makeQuery.apply("foo|2 bar|1", 1), 2);
        assertEquals(2, topDocs.scoreDocs.length);
        assertEquals(1L, topDocs.scoreDocs[0].doc);
        assertEquals(0L, topDocs.scoreDocs[1].doc);
        assertThat(topDocs.scoreDocs[0].score, greaterThan(topDocs.scoreDocs[1].score));

        // Doc 0 should get higher score than doc 1 as 'bar' has higher
        // frequency in the 'like' text
        topDocs = searcher.search(makeQuery.apply("foo|1 bar|2", 1), 2);
        assertEquals(2, topDocs.scoreDocs.length);
        assertEquals(0L, topDocs.scoreDocs[0].doc);
        assertEquals(1L, topDocs.scoreDocs[1].doc);
        assertThat(topDocs.scoreDocs[0].score, greaterThan(topDocs.scoreDocs[1].score));

        reader.close();
        indexWriter.close();
    }

    /**
     * A token filter that uses pipe '|' to denote term frequency, for example 'foo|3'
     * would be parsed as term 'foo' with frequency of 3.
     */
    private static class CustomFrequencyTokenFilterFactory extends TokenFilterFactory {
        CustomFrequencyTokenFilterFactory(Map<String, String> args) {
            super(args);
        }

        @Override
        public TokenStream create(TokenStream input) {
            return new TokenFilter(input) {

                @Override
                public boolean incrementToken() throws IOException {
                    final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
                    final TermFrequencyAttribute tfAtt = addAttribute(TermFrequencyAttribute.class);

                    if (input.incrementToken()) {
                        final char[] buffer = termAtt.buffer();
                        final int length = termAtt.length();
                        for (int i = 0; i < length; i++) {
                            if (buffer[i] == '|') {
                                termAtt.setLength(i);
                                i++;
                                tfAtt.setTermFrequency(ArrayUtil.parseInt(buffer, i, length - i));
                                return true;
                            }
                        }
                        return true;
                    }
                    return false;
                }
            };
        }
    }

    /**
     * An analyzer that tokenizes by whitespace and uses pipe '|' to denote term frequency.
     * For example 'foo|2 bar|3' would be first tokenized as 'foo|2', 'bar|3' and then
     * custom frequencies interpreted as 'foo': 2, 'bar': 3
     */
    private static class CustomFrequencyAnalyzer extends Analyzer {
        private final TokenFilterFactory factory;

        CustomFrequencyAnalyzer() {
            this.factory = new CustomFrequencyTokenFilterFactory(Collections.emptyMap());
        }

        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
            WhitespaceTokenizer tokenizer = new WhitespaceTokenizer();
            return new TokenStreamComponents(tokenizer, factory.create(tokenizer));
        }

        @Override
        protected TokenStream normalize(String fieldName, TokenStream in) {
            return factory.create(in);
        }
    }
}
