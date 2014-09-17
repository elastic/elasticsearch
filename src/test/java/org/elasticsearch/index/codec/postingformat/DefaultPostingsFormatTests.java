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

package org.elasticsearch.index.codec.postingformat;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene410.Lucene410Codec;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.codec.postingsformat.BloomFilterPostingsFormat;
import org.elasticsearch.index.codec.postingsformat.Elasticsearch090PostingsFormat;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.*;

/**
 * Simple smoke test for {@link org.elasticsearch.index.codec.postingsformat.Elasticsearch090PostingsFormat}
 */
public class DefaultPostingsFormatTests extends ElasticsearchTestCase {

    private final class TestCodec extends Lucene410Codec {

        @Override
        public PostingsFormat getPostingsFormatForField(String field) {
            return new Elasticsearch090PostingsFormat();
        }
    }

    @Test
    public void testUseDefault() throws IOException {
       
        Codec codec = new TestCodec();
        Directory d = new RAMDirectory();
        IndexWriterConfig config = new IndexWriterConfig(Lucene.VERSION, new WhitespaceAnalyzer(Lucene.VERSION));
        config.setCodec(codec);
        IndexWriter writer = new IndexWriter(d, config);
        writer.addDocument(Arrays.asList(new TextField("foo", "bar", Store.YES), new TextField(UidFieldMapper.NAME, "1234", Store.YES)));
        writer.commit();
        DirectoryReader reader = DirectoryReader.open(writer, false);
        List<AtomicReaderContext> leaves = reader.leaves();
        assertThat(leaves.size(), equalTo(1));
        AtomicReader ar = leaves.get(0).reader();
        Terms terms = ar.terms("foo");
        Terms uidTerms = ar.terms(UidFieldMapper.NAME);

        assertThat(terms.size(), equalTo(1l));
        assertThat(terms, not(instanceOf(BloomFilterPostingsFormat.BloomFilteredTerms.class)));
        assertThat(uidTerms, instanceOf(BloomFilterPostingsFormat.BloomFilteredTerms.class));

        reader.close();
        writer.close();
        d.close();
    }
    
    @Test
    public void testNoUIDField() throws IOException {
       
        Codec codec = new TestCodec();
        Directory d = new RAMDirectory();
        IndexWriterConfig config = new IndexWriterConfig(Lucene.VERSION, new WhitespaceAnalyzer(Lucene.VERSION));
        config.setCodec(codec);
        IndexWriter writer = new IndexWriter(d, config);
        for (int i = 0; i < 100; i++) {
            writer.addDocument(Arrays.asList(new TextField("foo", "foo bar foo bar", Store.YES), new TextField("some_other_field", "1234", Store.YES)));
        }
        writer.forceMerge(1, true);
        writer.commit();
        
        DirectoryReader reader = DirectoryReader.open(writer, false);
        List<AtomicReaderContext> leaves = reader.leaves();
        assertThat(leaves.size(), equalTo(1));
        AtomicReader ar = leaves.get(0).reader();
        Terms terms = ar.terms("foo");
        Terms some_other_field = ar.terms("some_other_field");

        assertThat(terms.size(), equalTo(2l));
        assertThat(terms, not(instanceOf(BloomFilterPostingsFormat.BloomFilteredTerms.class)));
        assertThat(some_other_field, not(instanceOf(BloomFilterPostingsFormat.BloomFilteredTerms.class)));
        TermsEnum iterator = terms.iterator(null);
        Set<String> expected = new HashSet<>();
        expected.add("foo");
        expected.add("bar");
        while(iterator.next() != null) {
            expected.remove(iterator.term().utf8ToString());
        }
        assertThat(expected.size(), equalTo(0));
        reader.close();
        writer.close();
        d.close();
    }

}
