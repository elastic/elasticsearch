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
package org.elasticsearch.search.fetch.subphase;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.ParentFieldMapper;
import org.elasticsearch.test.ESTestCase;

public class ParentFieldSubFetchPhaseTests extends ESTestCase {

    public void testGetParentId() throws Exception {
        ParentFieldMapper fieldMapper = createParentFieldMapper();
        Directory directory = newDirectory();
        IndexWriter indexWriter = new IndexWriter(directory, newIndexWriterConfig());
        Document document = new Document();
        document.add(new SortedDocValuesField(fieldMapper.fieldType().name(), new BytesRef("1")));
        indexWriter.addDocument(document);
        indexWriter.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        String id = ParentFieldSubFetchPhase.getParentId(fieldMapper, indexReader.leaves().get(0).reader(), 0);
        assertEquals("1", id);

        indexReader.close();
        directory.close();
    }

    public void testGetParentIdNoParentField() throws Exception {
        ParentFieldMapper fieldMapper = createParentFieldMapper();
        Directory directory = newDirectory();
        IndexWriter indexWriter = new IndexWriter(directory, newIndexWriterConfig());
        Document document = new Document();
        document.add(new SortedDocValuesField("different_field", new BytesRef("1")));
        indexWriter.addDocument(document);
        indexWriter.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        String id = ParentFieldSubFetchPhase.getParentId(fieldMapper, indexReader.leaves().get(0).reader(), 0);
        assertNull(id);

        indexReader.close();
        directory.close();
    }

    private ParentFieldMapper createParentFieldMapper() {
        Settings settings = Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .build();
        return new ParentFieldMapper.Builder("type")
                .type("parent_type")
                .build(new Mapper.BuilderContext(settings, new ContentPath(0)));
    }


}
