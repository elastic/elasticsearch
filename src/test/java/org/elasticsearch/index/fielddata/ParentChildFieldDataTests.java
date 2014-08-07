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

package org.elasticsearch.index.fielddata;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.search.MultiValueMode;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.*;

/**
 */
public class ParentChildFieldDataTests extends AbstractFieldDataTests {

    private final String parentType = "parent";
    private final String childType = "child";
    private final String grandChildType = "grand-child";

    @Before
    public void before() throws Exception {
        mapperService.merge(
                childType, new CompressedString(PutMappingRequest.buildFromSimplifiedDef(childType, "_parent", "type=" + parentType).string()), true
        );
        mapperService.merge(
                grandChildType, new CompressedString(PutMappingRequest.buildFromSimplifiedDef(grandChildType, "_parent", "type=" + childType).string()), true
        );

        Document d = new Document();
        d.add(new StringField(UidFieldMapper.NAME, Uid.createUid(parentType, "1"), Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField(UidFieldMapper.NAME, Uid.createUid(childType, "2"), Field.Store.NO));
        d.add(new StringField(ParentFieldMapper.NAME, Uid.createUid(parentType, "1"), Field.Store.NO));
        writer.addDocument(d);
        writer.commit();

        d = new Document();
        d.add(new StringField(UidFieldMapper.NAME, Uid.createUid(childType, "3"), Field.Store.NO));
        d.add(new StringField(ParentFieldMapper.NAME, Uid.createUid(parentType, "1"), Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField(UidFieldMapper.NAME, Uid.createUid(parentType, "2"), Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField(UidFieldMapper.NAME, Uid.createUid(childType, "4"), Field.Store.NO));
        d.add(new StringField(ParentFieldMapper.NAME, Uid.createUid(parentType, "2"), Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField(UidFieldMapper.NAME, Uid.createUid(childType, "5"), Field.Store.NO));
        d.add(new StringField(ParentFieldMapper.NAME, Uid.createUid(parentType, "1"), Field.Store.NO));
        writer.addDocument(d);
        writer.commit();

        d = new Document();
        d.add(new StringField(UidFieldMapper.NAME, Uid.createUid(grandChildType, "6"), Field.Store.NO));
        d.add(new StringField(ParentFieldMapper.NAME, Uid.createUid(childType, "2"), Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField(UidFieldMapper.NAME, Uid.createUid("other-type", "1"), Field.Store.NO));
        writer.addDocument(d);
    }

    @Test
    public void testGetBytesValues() throws Exception {
        IndexFieldData indexFieldData = getForField(childType);
        AtomicFieldData fieldData = indexFieldData.load(refreshReader());
        assertThat(fieldData.ramBytesUsed(), greaterThan(0l));

        SortedBinaryDocValues bytesValues = fieldData.getBytesValues();
        bytesValues.setDocument(0);
        assertThat(bytesValues.count(), equalTo(1));
        assertThat(bytesValues.valueAt(0).utf8ToString(), equalTo("1"));

        bytesValues.setDocument(1);
        assertThat(bytesValues.count(), equalTo(2));
        assertThat(bytesValues.valueAt(0).utf8ToString(), equalTo("1"));
        assertThat(bytesValues.valueAt(1).utf8ToString(), equalTo("2"));

        bytesValues.setDocument(2);
        assertThat(bytesValues.count(), equalTo(2));
        assertThat(bytesValues.valueAt(0).utf8ToString(), equalTo("1"));
        assertThat(bytesValues.valueAt(1).utf8ToString(), equalTo("3"));

        bytesValues.setDocument(3);
        assertThat(bytesValues.count(), equalTo(1));
        assertThat(bytesValues.valueAt(0).utf8ToString(), equalTo("2"));

        bytesValues.setDocument(4);
        assertThat(bytesValues.count(), equalTo(2));
        assertThat(bytesValues.valueAt(0).utf8ToString(), equalTo("2"));
        assertThat(bytesValues.valueAt(1).utf8ToString(), equalTo("4"));

        bytesValues.setDocument(5);
        assertThat(bytesValues.count(), equalTo(2));
        assertThat(bytesValues.valueAt(0).utf8ToString(), equalTo("1"));
        assertThat(bytesValues.valueAt(1).utf8ToString(), equalTo("5"));

        bytesValues.setDocument(6);
        assertThat(bytesValues.count(), equalTo(1));
        assertThat(bytesValues.valueAt(0).utf8ToString(), equalTo("2"));

        bytesValues.setDocument(7);
        assertThat(bytesValues.count(), equalTo(0));
    }

    @Test
    public void testSorting() throws Exception {
        IndexFieldData indexFieldData = getForField(childType);
        IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(writer, true));
        IndexFieldData.XFieldComparatorSource comparator = indexFieldData.comparatorSource("_last", MultiValueMode.MIN, null);

        TopFieldDocs topDocs = searcher.search(new MatchAllDocsQuery(), 10, new Sort(new SortField(ParentFieldMapper.NAME, comparator, false)));
        assertThat(topDocs.totalHits, equalTo(8));
        assertThat(topDocs.scoreDocs.length, equalTo(8));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(0));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[0]).fields[0]).utf8ToString(), equalTo("1"));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(1));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[1]).fields[0]).utf8ToString(), equalTo("1"));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(2));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[2]).fields[0]).utf8ToString(), equalTo("1"));
        assertThat(topDocs.scoreDocs[3].doc, equalTo(5));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[3]).fields[0]).utf8ToString(), equalTo("1"));
        assertThat(topDocs.scoreDocs[4].doc, equalTo(3));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[4]).fields[0]).utf8ToString(), equalTo("2"));
        assertThat(topDocs.scoreDocs[5].doc, equalTo(4));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[5]).fields[0]).utf8ToString(), equalTo("2"));
        assertThat(topDocs.scoreDocs[6].doc, equalTo(6));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[6]).fields[0]).utf8ToString(), equalTo("2"));
        assertThat(topDocs.scoreDocs[7].doc, equalTo(7));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[7]).fields[0]), equalTo(XFieldComparatorSource.MAX_TERM));

        topDocs = searcher.search(new MatchAllDocsQuery(), 10, new Sort(new SortField(ParentFieldMapper.NAME, comparator, true)));
        assertThat(topDocs.totalHits, equalTo(8));
        assertThat(topDocs.scoreDocs.length, equalTo(8));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(3));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[0]).fields[0]).utf8ToString(), equalTo("2"));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(4));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[1]).fields[0]).utf8ToString(), equalTo("2"));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(6));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[2]).fields[0]).utf8ToString(), equalTo("2"));
        assertThat(topDocs.scoreDocs[3].doc, equalTo(0));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[3]).fields[0]).utf8ToString(), equalTo("1"));
        assertThat(topDocs.scoreDocs[4].doc, equalTo(1));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[4]).fields[0]).utf8ToString(), equalTo("1"));
        assertThat(topDocs.scoreDocs[5].doc, equalTo(2));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[5]).fields[0]).utf8ToString(), equalTo("1"));
        assertThat(topDocs.scoreDocs[6].doc, equalTo(5));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[6]).fields[0]).utf8ToString(), equalTo("1"));
        assertThat(topDocs.scoreDocs[7].doc, equalTo(7));
        assertThat(((FieldDoc) topDocs.scoreDocs[7]).fields[0], nullValue());
    }

    @Override
    protected FieldDataType getFieldDataType() {
        return new FieldDataType("_parent");
    }
}
