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
package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.test.IndexSettingsModule;
import org.junit.Before;

import java.io.IOException;

public class FieldNamesFieldTypeTests extends FieldTypeTestCase {
    @Override
    protected MappedFieldType createDefaultFieldType() {
        return new FieldNamesFieldMapper.FieldNamesFieldType();
    }

    @Before
    public void setupProperties() {
        addModifier(new Modifier("enabled", true) {
            @Override
            public void modify(MappedFieldType ft) {
                FieldNamesFieldMapper.FieldNamesFieldType fnft = (FieldNamesFieldMapper.FieldNamesFieldType)ft;
                fnft.setEnabled(!fnft.isEnabled());
            }
        });
    }

    public void testTermQuery() throws IOException {
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
        Document doc = new Document();
        StringField fieldNames = new StringField(FieldNamesFieldMapper.CONTENT_TYPE, "field_name", Store.NO);
        doc.add(fieldNames);
        w.addDocument(doc);
        IndexReader reader = DirectoryReader.open(w);
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings(randomAlphaOfLengthBetween(1, 10), Settings.EMPTY);
        QueryShardContext context = new QueryShardContext(0, idxSettings, null, null, null, null, null, null, null, reader, null);
        
        FieldNamesFieldMapper.FieldNamesFieldType type = new FieldNamesFieldMapper.FieldNamesFieldType();
        type.setName(FieldNamesFieldMapper.CONTENT_TYPE);
        type.setEnabled(true);

        Query termQuery = type.termQuery("field_name", context);
        // all docs have the field
        assertEquals(new MatchAllDocsQuery(), termQuery);

        fieldNames.setStringValue("random_field_name");
        w.addDocument(doc);
        reader.close();
        reader = DirectoryReader.open(w);
        context = new QueryShardContext(0, idxSettings, null, null, null, null, null, null, null, reader, null);

        termQuery = type.termQuery("field_name", context);
        // not all docs have the field
        assertEquals(new ConstantScoreQuery(new TermQuery(new Term(FieldNamesFieldMapper.CONTENT_TYPE, "field_name"))), termQuery);

        type.setEnabled(false);
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> type.termQuery("field_name", null));
        assertEquals("Cannot run [exists] queries if the [_field_names] field is disabled", e.getMessage());

        IOUtils.close(reader, w, dir);
    }
}
