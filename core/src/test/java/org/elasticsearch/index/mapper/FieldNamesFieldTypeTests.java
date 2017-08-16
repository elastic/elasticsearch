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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper.FieldExistsQuery;
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
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings(randomAlphaOfLengthBetween(1, 10), Settings.EMPTY);
        QueryShardContext context = new QueryShardContext(
                0, idxSettings, null, null, null, null, null, null, null, null, null, null, null);
        
        FieldNamesFieldMapper.FieldNamesFieldType type = new FieldNamesFieldMapper.FieldNamesFieldType();
        type.setName(FieldNamesFieldMapper.CONTENT_TYPE);
        type.setEnabled(true);

        Query termQuery = type.termQuery("field_name", context);
        // all docs have the field
        assertEquals(new FieldExistsQuery(new BytesRef("field_name")), termQuery);

        type.setEnabled(false);
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> type.termQuery("field_name", null));
        assertEquals("Cannot run [exists] queries if the [_field_names] field is disabled", e.getMessage());
    }

    public void testFieldExistsQueryEqualsAndHashcode() {
        FieldExistsQuery q1 = new FieldExistsQuery(new BytesRef("foo"));
        FieldExistsQuery q2 = new FieldExistsQuery(new BytesRef("foo"));
        FieldExistsQuery q3 = new FieldExistsQuery(new BytesRef("bar"));
        QueryUtils.checkEqual(q1, q2);
        QueryUtils.checkUnequal(q1, q3);
    }

    public void testFieldExistsQuery() throws Exception {
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
        Document doc = new Document();
        doc.add(new StringField("_field_names", "foo", Store.NO));
        w.addDocument(doc);
        doc.add(new StringField("_field_names", "bar", Store.NO));
        w.addDocument(doc);
        w.forceMerge(1);
        IndexReader reader = DirectoryReader.open(w);
        w.close();
        IndexSearcher searcher = new IndexSearcher(reader);
        assertEquals(new MatchAllDocsQuery(), searcher.rewrite(new FieldExistsQuery(new BytesRef("foo"))));
        assertEquals(new ConstantScoreQuery(new TermQuery(new Term("_field_names", "bar"))),
                searcher.rewrite(new FieldExistsQuery(new BytesRef("bar"))));
        assertEquals(new MatchNoDocsQuery(), searcher.rewrite(new FieldExistsQuery(new BytesRef("quux"))));
        IOUtils.close(reader, dir);
    }
}
