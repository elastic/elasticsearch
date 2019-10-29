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

package org.elasticsearch.monitor.jvm;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.common.lucene.store.FilterIndexOutput;
import org.elasticsearch.test.ESTestCase;

public class StackTraceElementTaggerTests extends ESTestCase {

    private static void collectTags(Set<String> tags) {
        for (StackTraceElement ste : new RuntimeException().getStackTrace()) {
            String tag = StackTraceElementTagger.tag(ste);
            if (tag != null) {
                tags.add(tag);
            }
        }
    }

    private static class TagCollectingIndexOutput extends FilterIndexOutput {

        private final Set<String> tags;

        TagCollectingIndexOutput(IndexOutput out, Set<String> tags) {
            super(out.toString(), out);
            this.tags = tags;
        }

        @Override
        public void writeBytes(byte[] b, int offset, int length) throws IOException {
            super.writeBytes(b, offset, length);
            collectTags(tags);
        }
    }

    private static class TagCollectingIndexInput extends IndexInput {

        private final IndexInput in;
        private final Set<String> tags;

        protected TagCollectingIndexInput(IndexInput in, Set<String> tags) {
            super(in.toString());
            this.in = in;
            this.tags = tags;
        }

        @Override
        public void close() throws IOException {
            in.close();
        }

        @Override
        public long getFilePointer() {
            return in.getFilePointer();
        }

        @Override
        public void seek(long pos) throws IOException {
            in.seek(pos);
        }

        @Override
        public long length() {
            return in.length();
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
            return new TagCollectingIndexInput(in.slice(sliceDescription, offset, length), tags);
        }

        @Override
        public byte readByte() throws IOException {
            collectTags(tags);
            return in.readByte();
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            collectTags(tags);
            in.readBytes(b, offset, len);
        }
        
    }

    private static class TagCollectingDirectory extends FilterDirectory {

        private final Set<String> tags;

        TagCollectingDirectory(Directory dir, Set<String> tags) {
            super(dir);
            this.tags = tags;
        }

        @Override
        public IndexOutput createOutput(String name, IOContext context) throws IOException {
            collectTags(tags);
            return new TagCollectingIndexOutput(super.createOutput(name, context), tags);
        }

        @Override
        public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
            collectTags(tags);
            return new TagCollectingIndexOutput(super.createTempOutput(prefix, suffix, context), tags);
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            return new TagCollectingIndexInput(super.openInput(name, context), tags);
        }

        @Override
        public ChecksumIndexInput openChecksumInput(String name, IOContext context) throws IOException {
            return new BufferedChecksumIndexInput(openInput(name, context));
        }
    }

    private static class TagCollectingAnalyzer extends Analyzer {

        private final Set<String> tags;

        TagCollectingAnalyzer(Set<String> tags) {
            this.tags = tags;
        }

        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
            Tokenizer tokenizer = new Tokenizer() {

                private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
                private boolean done = true;
               
                @Override
                public boolean incrementToken() throws IOException {
                    collectTags(tags);
                    if (done == false) {
                        termAtt.setEmpty().append("foobar");
                        return true;
                    }
                    return false;
                }

            };
            return new TokenStreamComponents(tokenizer);
        }
    }

    public void testAppend() throws IOException {
        Set<String> tags = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
        try (Directory dir = new TagCollectingDirectory(newDirectory(), tags);
                IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(new TagCollectingAnalyzer(tags)))) {
            assertEquals(Collections.emptySet(), tags);
            Document doc = new Document();
            doc.add(new StringField("foo", "bar", Store.NO));
            w.addDocument(doc);
            assertEquals(Set.of("APPEND_OR_OVERWRITE_DOC", "APPEND_DOC"), tags);
            tags.clear();
        }
        assertEquals(Set.of("CREATE_SEGMENT"), tags);
    }

    public void testUpdate() throws IOException {
        Set<String> tags = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
        try (Directory dir = new TagCollectingDirectory(newDirectory(), tags);
                IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(new TagCollectingAnalyzer(tags)))) {
            assertEquals(Collections.emptySet(), tags);
            Document doc = new Document();
            doc.add(new StringField("foo", "bar", Store.NO));
            w.updateDocument(new Term("id", "0"), doc);
            assertEquals(Set.of("APPEND_OR_OVERWRITE_DOC"), tags);
            tags.clear();
        }
        assertEquals(Set.of("CREATE_SEGMENT"), tags);
    }

    public void testAppendWithAnalysis() throws IOException {
        Set<String> tags = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
        try (Directory dir = new TagCollectingDirectory(newDirectory(), tags);
                IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(new TagCollectingAnalyzer(tags)))) {
            assertEquals(Collections.emptySet(), tags);
            Document doc = new Document();
            doc.add(new TextField("foo", "bar", Store.NO));
            w.addDocument(doc);
            assertEquals(Set.of("APPEND_OR_OVERWRITE_DOC", "APPEND_DOC", "ANALYSIS"), tags);
            tags.clear();
        }
        assertEquals(Set.of("CREATE_SEGMENT"), tags);
    }

    public void testRefresh() throws IOException {
        Set<String> tags = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
        try (Directory dir = new TagCollectingDirectory(newDirectory(), tags);
                IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(new TagCollectingAnalyzer(tags)))) {
            assertEquals(Collections.emptySet(), tags);
            w.addDocument(new Document());
            assertEquals(Set.of("APPEND_OR_OVERWRITE_DOC", "APPEND_DOC"), tags);
            tags.clear();
            DirectoryReader.open(w).close();
            assertEquals(Set.of("CREATE_SEGMENT", "REFRESH_SHARD"), tags);
            tags.clear();
        }
        assertEquals(Set.of(), tags);
    }

    public void testMerge() throws IOException {
        Set<String> tags = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
        try (Directory dir = new TagCollectingDirectory(newDirectory(), tags);
                IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(new TagCollectingAnalyzer(tags)))) {
            assertEquals(Collections.emptySet(), tags);
            w.addDocument(new Document());
            assertEquals(Set.of("APPEND_OR_OVERWRITE_DOC", "APPEND_DOC"), tags);
            tags.clear();
            DirectoryReader.open(w).close();
            assertEquals(Set.of("CREATE_SEGMENT", "REFRESH_SHARD"), tags);
            tags.clear();
            w.addDocument(new Document());
            assertEquals(Set.of("APPEND_OR_OVERWRITE_DOC", "APPEND_DOC"), tags);
            tags.clear();
            DirectoryReader.open(w).close();
            assertEquals(Set.of("CREATE_SEGMENT", "REFRESH_SHARD"), tags);
            tags.clear();
            w.forceMerge(1);
            assertEquals(Set.of("MERGE_SEGMENT"), tags);
            tags.clear();
        }
        assertEquals(Set.of(), tags);
    }

    public void testDVUpdates() throws IOException {
        Set<String> tags = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
        try (Directory dir = new TagCollectingDirectory(newDirectory(), tags);
                IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(new TagCollectingAnalyzer(tags)))) {
            assertEquals(Collections.emptySet(), tags);
            Document doc = new Document();
            doc.add(new StringField("_id", "0", Store.NO));
            doc.add(new NumericDocValuesField("dv", 1L));
            w.addDocument(doc);
            assertEquals(Set.of("APPEND_OR_OVERWRITE_DOC", "APPEND_DOC"), tags);
            tags.clear();
            DirectoryReader.open(w).close();
            assertEquals(Set.of("CREATE_SEGMENT", "REFRESH_SHARD"), tags);
            tags.clear();
            w.updateNumericDocValue(new Term("_id", "0"), "dv", 3L);
            DirectoryReader.open(w).close();
            assertEquals(Set.of("WRITE_SEGMENT_UPDATES", "REFRESH_SHARD"), tags);
            tags.clear();
        }
        assertEquals(Set.of(), tags);
    }
}
