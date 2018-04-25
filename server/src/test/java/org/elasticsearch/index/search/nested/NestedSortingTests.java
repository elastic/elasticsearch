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

package org.elasticsearch.index.search.nested;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.fielddata.AbstractFieldDataTestCase;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource;
import org.elasticsearch.index.fielddata.NoOrdinalsStringFieldDataTests;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.index.fielddata.plain.PagedBytesIndexFieldData;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class NestedSortingTests extends AbstractFieldDataTestCase {
    @Override
    protected String getFieldDataType() {
        return "string";
    }

    public void testDuel() throws Exception {
        final int numDocs = scaledRandomIntBetween(100, 1000);
        for (int i = 0; i < numDocs; ++i) {
            final int numChildren = randomInt(2);
            List<Document> docs = new ArrayList<>(numChildren + 1);
            for (int j = 0; j < numChildren; ++j) {
                Document doc = new Document();
                doc.add(new StringField("f", TestUtil.randomSimpleString(random(), 2), Field.Store.NO));
                doc.add(new StringField("__type", "child", Field.Store.NO));
                docs.add(doc);
            }
            if (randomBoolean()) {
                docs.add(new Document());
            }
            Document parent = new Document();
            parent.add(new StringField("__type", "parent", Field.Store.NO));
            docs.add(parent);
            writer.addDocuments(docs);
            if (rarely()) { // we need to have a bit more segments than what RandomIndexWriter would do by default
                DirectoryReader.open(writer).close();
            }
        }
        writer.commit();

        MultiValueMode sortMode = randomFrom(Arrays.asList(MultiValueMode.MIN, MultiValueMode.MAX));
        DirectoryReader reader = DirectoryReader.open(writer);
        reader = ElasticsearchDirectoryReader.wrap(reader, new ShardId(indexService.index(), 0));
        IndexSearcher searcher = new IndexSearcher(reader);
        PagedBytesIndexFieldData indexFieldData1 = getForField("f");
        IndexFieldData<?> indexFieldData2 = NoOrdinalsStringFieldDataTests.hideOrdinals(indexFieldData1);
        final String missingValue = randomBoolean() ? null : TestUtil.randomSimpleString(random(), 2);
        final int n = randomIntBetween(1, numDocs + 2);
        final boolean reverse = randomBoolean();

        final TopDocs topDocs1 = getTopDocs(searcher, indexFieldData1, missingValue, sortMode, n, reverse);
        final TopDocs topDocs2 = getTopDocs(searcher, indexFieldData2, missingValue, sortMode, n, reverse);
        for (int i = 0; i < topDocs1.scoreDocs.length; ++i) {
            final FieldDoc fieldDoc1 = (FieldDoc) topDocs1.scoreDocs[i];
            final FieldDoc fieldDoc2 = (FieldDoc) topDocs2.scoreDocs[i];
            assertEquals(fieldDoc1.doc, fieldDoc2.doc);
            assertArrayEquals(fieldDoc1.fields, fieldDoc2.fields);
        }

        searcher.getIndexReader().close();
    }

    private TopDocs getTopDocs(IndexSearcher searcher, IndexFieldData<?> indexFieldData, String missingValue, MultiValueMode sortMode, int n, boolean reverse) throws IOException {
        Query parentFilter = new TermQuery(new Term("__type", "parent"));
        Query childFilter = new TermQuery(new Term("__type", "child"));
        SortField sortField = indexFieldData.sortField(missingValue, sortMode, createNested(searcher, parentFilter, childFilter), reverse);
        Query query = new ConstantScoreQuery(parentFilter);
        Sort sort = new Sort(sortField);
        return searcher.search(query, n, sort);
    }

    public void testNestedSorting() throws Exception {
        List<Document> docs = new ArrayList<>();
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

        MultiValueMode sortMode = MultiValueMode.MIN;
        DirectoryReader reader = DirectoryReader.open(writer);
        reader = ElasticsearchDirectoryReader.wrap(reader, new ShardId(indexService.index(), 0));
        IndexSearcher searcher = new IndexSearcher(reader);
        PagedBytesIndexFieldData indexFieldData = getForField("field2");
        Query parentFilter = new TermQuery(new Term("__type", "parent"));
        Query childFilter = Queries.not(parentFilter);
        BytesRefFieldComparatorSource nestedComparatorSource = new BytesRefFieldComparatorSource(indexFieldData, null, sortMode, createNested(searcher, parentFilter, childFilter));
        ToParentBlockJoinQuery query = new ToParentBlockJoinQuery(new ConstantScoreQuery(childFilter), new QueryBitSetProducer(parentFilter), ScoreMode.None);

        Sort sort = new Sort(new SortField("field2", nestedComparatorSource));
        TopFieldDocs topDocs = searcher.search(query, 5, sort);
        assertThat(topDocs.totalHits, equalTo(7L));
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

        sortMode = MultiValueMode.MAX;
        nestedComparatorSource = new BytesRefFieldComparatorSource(indexFieldData, null, sortMode, createNested(searcher, parentFilter, childFilter));
        sort = new Sort(new SortField("field2", nestedComparatorSource, true));
        topDocs = searcher.search(query, 5, sort);
        assertThat(topDocs.totalHits, equalTo(7L));
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


        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        bq.add(parentFilter, Occur.MUST_NOT);
        bq.add(new TermQuery(new Term("filter_1", "T")), Occur.MUST);
        childFilter = bq.build();
        nestedComparatorSource = new BytesRefFieldComparatorSource(indexFieldData, null, sortMode, createNested(searcher, parentFilter, childFilter));
        query = new ToParentBlockJoinQuery(
                new ConstantScoreQuery(childFilter),
                new QueryBitSetProducer(parentFilter),
                ScoreMode.None
        );
        sort = new Sort(new SortField("field2", nestedComparatorSource, true));
        topDocs = searcher.search(query, 5, sort);
        assertThat(topDocs.totalHits, equalTo(6L));
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
