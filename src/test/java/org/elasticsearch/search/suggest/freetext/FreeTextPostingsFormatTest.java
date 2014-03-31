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
package org.elasticsearch.search.suggest.freetext;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.suggest.DocumentDictionary;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.analyzing.FreeTextSuggester;
import org.apache.lucene.search.suggest.analyzing.XFreeTextSuggester;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.fst.FST;
import org.elasticsearch.Version;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 *
 */
public class FreeTextPostingsFormatTest extends ElasticsearchTestCase {

    @Test
    public void testThatFreetextSuggesterIsWorking() throws Exception {
        Directory directory = new RAMDirectory();
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(Version.CURRENT.luceneVersion, new StandardAnalyzer(Version.CURRENT.luceneVersion));
        IndexWriter indexWriter = new IndexWriter(directory, indexWriterConfig);
        Document document = new Document();
        document.add(new StringField("foo", "sind wir nicht alle ein bischen bluna", Field.Store.YES));
        document.add(new IntField("weight", 12, Field.Store.YES));
        indexWriter.addDocument(document);
        indexWriter.commit();

        DirectoryReader reader = DirectoryReader.open(directory);
        DocumentDictionary dictionary = new DocumentDictionary(reader, "foo", "weight");

        Analyzer analyzer = new WhitespaceAnalyzer(Version.CURRENT.luceneVersion);
        FreeTextSuggester freetextSuggester = new FreeTextSuggester(analyzer, analyzer, 2, (byte)' ');
        freetextSuggester.build(dictionary);

        List<Lookup.LookupResult> lookup = freetextSuggester.lookup("sind w", 1);
        assertThat(lookup.get(0).key.toString(), is("sind wir"));

        lookup = freetextSuggester.lookup("sind a", 1);
        assertThat(lookup.get(0).key.toString(), is("alle"));
    }

    @Test
    public void freeTextSuggesterBuilderTest() throws Exception {
        XFreeTextSuggester.XBuilder builder = new XFreeTextSuggester.XBuilder();
        builder.startTerm(new BytesRef("bar"));
        builder.finishTerm(222);
        builder.startTerm(new BytesRef("foo"));
        builder.finishTerm(123);
        builder.startTerm(new BytesRef("foob"));
        builder.finishTerm(80);
        FST<Long> build = builder.build();

        Analyzer analyzer = new WhitespaceAnalyzer(Version.CURRENT.luceneVersion);
        XFreeTextSuggester freeTextSuggester = new XFreeTextSuggester(analyzer, analyzer,
                XFreeTextSuggester.DEFAULT_GRAMS, XFreeTextSuggester.DEFAULT_SEPARATOR, build, 3, 20);

        List<Lookup.LookupResult> results = freeTextSuggester.lookup("f", 10);
        assertThat(results, hasSize(2));
        assertThat(results.get(0).key.toString(), is("foo"));
        assertThat(results.get(1).key.toString(), is("foob"));
    }

    @Test
    public void freeTextSuggesterBuilderSeparatorTest() throws Exception {
        XFreeTextSuggester.XBuilder builder = new XFreeTextSuggester.XBuilder();
        builder.startTerm(new BytesRef("foo "));
        builder.finishTerm(123);
        builder.startTerm(new BytesRef("foo bar"));
        builder.finishTerm(80);
        FST<Long> build = builder.build();

        Analyzer analyzer = new WhitespaceAnalyzer(Version.CURRENT.luceneVersion);
        XFreeTextSuggester freeTextSuggester = new XFreeTextSuggester(analyzer, analyzer,
                XFreeTextSuggester.DEFAULT_GRAMS, XFreeTextSuggester.DEFAULT_SEPARATOR, build, 3, 20);

        List<Lookup.LookupResult> results = freeTextSuggester.lookup("f", 10);
        assertThat(results, hasSize(2));
        assertThat(results.get(0).key.toString(), is("foo "));
        assertThat(results.get(1).key.toString(), is("foo bar"));
    }
}
