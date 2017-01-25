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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.junit.Before;

import java.io.IOException;

public class TypeFieldTypeTests extends FieldTypeTestCase {
    @Override
    protected MappedFieldType createDefaultFieldType() {
        return new TypeFieldMapper.TypeFieldType();
    }

    @Before
    public void setupProperties() {
        addModifier(new Modifier("fielddata", true) {
            @Override
            public void modify(MappedFieldType ft) {
                TypeFieldMapper.TypeFieldType tft = (TypeFieldMapper.TypeFieldType) ft;
                tft.setFielddata(tft.fielddata() == false);
            }
        });
    }

    public void testTermsQuery() throws Exception {
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
        IndexReader reader = openReaderWithNewType("my_type", w);

        TypeFieldMapper.TypeFieldType ft = new TypeFieldMapper.TypeFieldType();
        ft.setName(TypeFieldMapper.NAME);
        Query query = ft.termQuery("my_type", null);
        assertEquals(new MatchAllDocsQuery(), query.rewrite(reader));

        // Make sure that Lucene actually simplifies the query when there is a single type
        Query userQuery = new PhraseQuery("body", "quick", "fox");
        Query filteredQuery = new BooleanQuery.Builder().add(userQuery, Occur.MUST).add(query, Occur.FILTER).build();
        Query rewritten = new IndexSearcher(reader).rewrite(filteredQuery);
        assertEquals(userQuery, rewritten);

        // ... and does not rewrite it if there is more than one type
        reader.close();
        reader = openReaderWithNewType("my_type2", w);
        Query expected = new ConstantScoreQuery(
            new BooleanQuery.Builder()
                .add(new TermQuery(new Term(TypeFieldMapper.NAME, "my_type")), Occur.SHOULD)
            .build()
        );
        assertEquals(expected, query.rewrite(reader));

        BytesRef[] types =
            new BytesRef[] {new BytesRef("my_type"), new BytesRef("my_type2"), new BytesRef("my_type3")};
        // the query should match all documents
        query = new TypeFieldMapper.TypesQuery(types);
        assertEquals(new MatchAllDocsQuery(), query.rewrite(reader));

        reader.close();
        reader = openReaderWithNewType("unknown_type", w);
        // the query cannot rewrite to a match all docs sinc unknown_type is not queried.
        query = new TypeFieldMapper.TypesQuery(types);
        expected =
            new ConstantScoreQuery(
                new BooleanQuery.Builder()
                    .add(new TermQuery(new Term(TypeFieldMapper.CONTENT_TYPE, types[0])), Occur.SHOULD)
                    .add(new TermQuery(new Term(TypeFieldMapper.CONTENT_TYPE, types[1])), Occur.SHOULD)
                .build()
            );
        rewritten = query.rewrite(reader);
        assertEquals(expected, rewritten);

        // make sure that redundant types does not rewrite to MatchAllDocsQuery
        query = new TypeFieldMapper.TypesQuery(new BytesRef("my_type"), new BytesRef("my_type"), new BytesRef("my_type"));
        expected =
            new ConstantScoreQuery(
                new BooleanQuery.Builder()
                    .add(new TermQuery(new Term(TypeFieldMapper.CONTENT_TYPE, "my_type")), Occur.SHOULD)
                    .build()
            );
        rewritten = query.rewrite(reader);
        assertEquals(expected, rewritten);

        IOUtils.close(reader, w, dir);
    }

    static DirectoryReader openReaderWithNewType(String type, IndexWriter writer) throws IOException {
        Document doc = new Document();
        StringField typeField = new StringField(TypeFieldMapper.NAME, type, Store.NO);
        doc.add(typeField);
        writer.addDocument(doc);
        return DirectoryReader.open(writer);
    }
}
