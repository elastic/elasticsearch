/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


package org.elasticsearch.index.search.nested;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.search.*;
import org.apache.lucene.search.join.FixedBitSetCachingWrapperFilter;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.search.AndFilter;
import org.elasticsearch.common.lucene.search.NotFilter;
import org.elasticsearch.common.lucene.search.XFilteredQuery;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.fielddata.AbstractFieldDataTests;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.index.fielddata.fieldcomparator.SortMode;
import org.elasticsearch.index.fielddata.plain.PagedBytesIndexFieldData;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

/**
 */
public class NestedSortingTests extends AbstractFieldDataTests {

    @Override
    protected FieldDataType getFieldDataType() {
        return new FieldDataType("string", ImmutableSettings.builder().put("format", "paged_bytes"));
    }

    @Test
    public void testNestedSorting() throws Exception {
        List<Document> docs = new ArrayList<Document>();
        Document document = new Document();
        document.add(new StringField("field2", "a", Field.Store.NO));
        document.add(new StringField("filter_1", "T", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField("field2", "b", Field.Store.NO));
        document.add(new StringField("filter_1", "T", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField("field2", "c", Field.Store.NO));
        document.add(new StringField("filter_1", "T", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField("__type", "parent", Field.Store.NO));
        document.add(new StringField("field1", "a", Field.Store.NO));
        docs.add(document);
        writer.addDocuments(docs);
        writer.commit();

        docs.clear();
        document = new Document();
        document.add(new StringField("field2", "c", Field.Store.NO));
        document.add(new StringField("filter_1", "T", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField("field2", "d", Field.Store.NO));
        document.add(new StringField("filter_1", "T", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField("field2", "e", Field.Store.NO));
        document.add(new StringField("filter_1", "T", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField("__type", "parent", Field.Store.NO));
        document.add(new StringField("field1", "b", Field.Store.NO));
        docs.add(document);
        writer.addDocuments(docs);

        docs.clear();
        document = new Document();
        document.add(new StringField("field2", "e", Field.Store.NO));
        document.add(new StringField("filter_1", "T", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField("field2", "f", Field.Store.NO));
        document.add(new StringField("filter_1", "T", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField("field2", "g", Field.Store.NO));
        document.add(new StringField("filter_1", "T", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField("__type", "parent", Field.Store.NO));
        document.add(new StringField("field1", "c", Field.Store.NO));
        docs.add(document);
        writer.addDocuments(docs);

        docs.clear();
        document = new Document();
        document.add(new StringField("field2", "g", Field.Store.NO));
        document.add(new StringField("filter_1", "T", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField("field2", "h", Field.Store.NO));
        document.add(new StringField("filter_1", "F", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField("field2", "i", Field.Store.NO));
        document.add(new StringField("filter_1", "F", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField("__type", "parent", Field.Store.NO));
        document.add(new StringField("field1", "d", Field.Store.NO));
        docs.add(document);
        writer.addDocuments(docs);
        writer.commit();

        docs.clear();
        document = new Document();
        document.add(new StringField("field2", "i", Field.Store.NO));
        document.add(new StringField("filter_1", "F", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField("field2", "j", Field.Store.NO));
        document.add(new StringField("filter_1", "F", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField("field2", "k", Field.Store.NO));
        document.add(new StringField("filter_1", "F", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField("__type", "parent", Field.Store.NO));
        document.add(new StringField("field1", "f", Field.Store.NO));
        docs.add(document);
        writer.addDocuments(docs);

        docs.clear();
        document = new Document();
        document.add(new StringField("field2", "k", Field.Store.NO));
        document.add(new StringField("filter_1", "T", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField("field2", "l", Field.Store.NO));
        document.add(new StringField("filter_1", "T", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField("field2", "m", Field.Store.NO));
        document.add(new StringField("filter_1", "T", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField("__type", "parent", Field.Store.NO));
        document.add(new StringField("field1", "g", Field.Store.NO));
        docs.add(document);
        writer.addDocuments(docs);

        // This doc will not be included, because it doesn't have nested docs
        document = new Document();
        document.add(new StringField("__type", "parent", Field.Store.NO));
        document.add(new StringField("field1", "h", Field.Store.NO));
        writer.addDocument(document);

        docs.clear();
        document = new Document();
        document.add(new StringField("field2", "m", Field.Store.NO));
        document.add(new StringField("filter_1", "T", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField("field2", "n", Field.Store.NO));
        document.add(new StringField("filter_1", "F", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField("field2", "o", Field.Store.NO));
        document.add(new StringField("filter_1", "F", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField("__type", "parent", Field.Store.NO));
        document.add(new StringField("field1", "i", Field.Store.NO));
        docs.add(document);
        writer.addDocuments(docs);
        writer.commit();

        // Some garbage docs, just to check if the NestedFieldComparator can deal with this.
        document = new Document();
        document.add(new StringField("fieldXXX", "x", Field.Store.NO));
        writer.addDocument(document);
        document = new Document();
        document.add(new StringField("fieldXXX", "x", Field.Store.NO));
        writer.addDocument(document);
        document = new Document();
        document.add(new StringField("fieldXXX", "x", Field.Store.NO));
        writer.addDocument(document);

        SortMode sortMode = SortMode.MIN;
        IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(writer, false));
        PagedBytesIndexFieldData indexFieldData = getForField("field2");
        BytesRefFieldComparatorSource innerSource = new BytesRefFieldComparatorSource(indexFieldData, null, sortMode);
        Filter parentFilter = new TermFilter(new Term("__type", "parent"));
        Filter childFilter = new NotFilter(parentFilter);
        NestedFieldComparatorSource nestedComparatorSource = new NestedFieldComparatorSource(sortMode, innerSource, parentFilter, childFilter);
        ToParentBlockJoinQuery query = new ToParentBlockJoinQuery(new XFilteredQuery(new MatchAllDocsQuery(), childFilter), new FixedBitSetCachingWrapperFilter(parentFilter), ScoreMode.None);

        Sort sort = new Sort(new SortField("field2", nestedComparatorSource));
        TopFieldDocs topDocs = searcher.search(query, 5, sort);
        assertThat(topDocs.totalHits, equalTo(7));
        assertThat(topDocs.scoreDocs.length, equalTo(5));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(3));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[0]).fields[0]).utf8ToString(), equalTo("a"));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(7));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[1]).fields[0]).utf8ToString(), equalTo("c"));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(11));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[2]).fields[0]).utf8ToString(), equalTo("e"));
        assertThat(topDocs.scoreDocs[3].doc, equalTo(15));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[3]).fields[0]).utf8ToString(), equalTo("g"));
        assertThat(topDocs.scoreDocs[4].doc, equalTo(19));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[4]).fields[0]).utf8ToString(), equalTo("i"));

        sortMode = SortMode.MAX;
        nestedComparatorSource = new NestedFieldComparatorSource(sortMode, innerSource, parentFilter, childFilter);
        sort = new Sort(new SortField("field2", nestedComparatorSource, true));
        topDocs = searcher.search(query, 5, sort);
        assertThat(topDocs.totalHits, equalTo(7));
        assertThat(topDocs.scoreDocs.length, equalTo(5));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(28));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[0]).fields[0]).utf8ToString(), equalTo("o"));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(23));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[1]).fields[0]).utf8ToString(), equalTo("m"));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(19));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[2]).fields[0]).utf8ToString(), equalTo("k"));
        assertThat(topDocs.scoreDocs[3].doc, equalTo(15));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[3]).fields[0]).utf8ToString(), equalTo("i"));
        assertThat(topDocs.scoreDocs[4].doc, equalTo(11));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[4]).fields[0]).utf8ToString(), equalTo("g"));


        childFilter = new AndFilter(Arrays.asList(new NotFilter(parentFilter), new TermFilter(new Term("filter_1", "T"))));
        nestedComparatorSource = new NestedFieldComparatorSource(sortMode, innerSource, parentFilter, childFilter);
        query = new ToParentBlockJoinQuery(
                new XFilteredQuery(new MatchAllDocsQuery(), childFilter),
                new FixedBitSetCachingWrapperFilter(parentFilter),
                ScoreMode.None
        );
        sort = new Sort(new SortField("field2", nestedComparatorSource, true));
        topDocs = searcher.search(query, 5, sort);
        assertThat(topDocs.totalHits, equalTo(6));
        assertThat(topDocs.scoreDocs.length, equalTo(5));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(23));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[0]).fields[0]).utf8ToString(), equalTo("m"));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(28));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[1]).fields[0]).utf8ToString(), equalTo("m"));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(11));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[2]).fields[0]).utf8ToString(), equalTo("g"));
        assertThat(topDocs.scoreDocs[3].doc, equalTo(15));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[3]).fields[0]).utf8ToString(), equalTo("g"));
        assertThat(topDocs.scoreDocs[4].doc, equalTo(7));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[4]).fields[0]).utf8ToString(), equalTo("e"));

        searcher.getIndexReader().close();
    }

}
