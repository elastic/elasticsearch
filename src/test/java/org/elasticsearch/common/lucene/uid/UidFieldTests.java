/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.lucene.uid;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.*;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.hamcrest.MatcherAssert;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 *
 */
public class UidFieldTests {

    @Test
    public void testUidField() throws Exception {
        IndexWriter writer = new IndexWriter(new RAMDirectory(), new IndexWriterConfig(Lucene.VERSION, Lucene.STANDARD_ANALYZER));

        DirectoryReader directoryReader = DirectoryReader.open(writer, true);
        AtomicReader atomicReader = SlowCompositeReaderWrapper.wrap(directoryReader);
        MatcherAssert.assertThat(UidField.loadVersion(atomicReader.getContext(), new Term("_uid", "1")), equalTo(-1l));

        Document doc = new Document();
        doc.add(new Field("_uid", "1", UidFieldMapper.Defaults.FIELD_TYPE));
        writer.addDocument(doc);
        directoryReader = DirectoryReader.openIfChanged(directoryReader);
        atomicReader = SlowCompositeReaderWrapper.wrap(directoryReader);
        assertThat(UidField.loadVersion(atomicReader.getContext(), new Term("_uid", "1")), equalTo(-2l));
        assertThat(UidField.loadDocIdAndVersion(atomicReader.getContext(), new Term("_uid", "1")).version, equalTo(-2l));

        doc = new Document();
        doc.add(new UidField("_uid", "1", 1));
        writer.updateDocument(new Term("_uid", "1"), doc);
        directoryReader = DirectoryReader.openIfChanged(directoryReader);
        atomicReader = SlowCompositeReaderWrapper.wrap(directoryReader);
        assertThat(UidField.loadVersion(atomicReader.getContext(), new Term("_uid", "1")), equalTo(1l));
        assertThat(UidField.loadDocIdAndVersion(atomicReader.getContext(), new Term("_uid", "1")).version, equalTo(1l));

        doc = new Document();
        UidField uid = new UidField("_uid", "1", 2);
        doc.add(uid);
        writer.updateDocument(new Term("_uid", "1"), doc);
        directoryReader = DirectoryReader.openIfChanged(directoryReader);
        atomicReader = SlowCompositeReaderWrapper.wrap(directoryReader);
        assertThat(UidField.loadVersion(atomicReader.getContext(), new Term("_uid", "1")), equalTo(2l));
        assertThat(UidField.loadDocIdAndVersion(atomicReader.getContext(), new Term("_uid", "1")).version, equalTo(2l));

        // test reuse of uid field
        doc = new Document();
        uid.version(3);
        doc.add(uid);
        writer.updateDocument(new Term("_uid", "1"), doc);
        directoryReader = DirectoryReader.openIfChanged(directoryReader);
        atomicReader = SlowCompositeReaderWrapper.wrap(directoryReader);
        assertThat(UidField.loadVersion(atomicReader.getContext(), new Term("_uid", "1")), equalTo(3l));
        assertThat(UidField.loadDocIdAndVersion(atomicReader.getContext(), new Term("_uid", "1")).version, equalTo(3l));

        writer.deleteDocuments(new Term("_uid", "1"));
        directoryReader = DirectoryReader.openIfChanged(directoryReader);
        atomicReader = SlowCompositeReaderWrapper.wrap(directoryReader);
        assertThat(UidField.loadVersion(atomicReader.getContext(), new Term("_uid", "1")), equalTo(-1l));
        assertThat(UidField.loadDocIdAndVersion(atomicReader.getContext(), new Term("_uid", "1")), nullValue());
    }
}
