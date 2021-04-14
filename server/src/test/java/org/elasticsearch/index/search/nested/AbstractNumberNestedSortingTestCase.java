/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.search.nested;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
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
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.fielddata.AbstractFieldDataTestCase;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractNumberNestedSortingTestCase extends AbstractFieldDataTestCase {

    @Override
    protected boolean hasDocValues() {
        return true;
    }

    public void testNestedSorting() throws Exception {
        List<Document> docs = new ArrayList<>();
        Document document = new Document();
        document.add(createField("field2", 3));
        document.add(new StringField("filter_1", "T", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(createField("field2", 3));
        document.add(new StringField("filter_1", "T", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(createField("field2", 3));
        document.add(new StringField("filter_1", "T", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField("__type", "parent", Field.Store.NO));
        document.add(createField("field1", 1));
        docs.add(document);
        writer.addDocuments(docs);
        writer.commit();

        docs.clear();
        document = new Document();
        document.add(createField("field2", 3));
        document.add(new StringField("filter_1", "T", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(createField("field2", 3));
        document.add(new StringField("filter_1", "T", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(createField("field2", 2));
        document.add(new StringField("filter_1", "T", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField("__type", "parent", Field.Store.NO));
        document.add(createField("field1", 2));
        docs.add(document);
        writer.addDocuments(docs);

        docs.clear();
        document = new Document();
        document.add(createField("field2", 3));
        document.add(new StringField("filter_1", "T", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(createField("field2", 3));
        document.add(new StringField("filter_1", "T", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(createField("field2", 1));
        document.add(new StringField("filter_1", "T", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField("__type", "parent", Field.Store.NO));
        document.add(createField("field1", 3));
        docs.add(document);
        writer.addDocuments(docs);

        docs.clear();
        document = new Document();
        document.add(createField("field2", 3));
        document.add(new StringField("filter_1", "T", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(createField("field2", 3));
        document.add(new StringField("filter_1", "F", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(createField("field2", 4));
        document.add(new StringField("filter_1", "F", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField("__type", "parent", Field.Store.NO));
        document.add(createField("field1", 4));
        docs.add(document);
        writer.addDocuments(docs);
        writer.commit();

        docs.clear();
        document = new Document();
        document.add(createField("field2", 3));
        document.add(new StringField("filter_1", "F", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(createField("field2", 3));
        document.add(new StringField("filter_1", "F", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(createField("field2", 5));
        document.add(new StringField("filter_1", "F", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField("__type", "parent", Field.Store.NO));
        document.add(createField("field1", 5));
        docs.add(document);
        writer.addDocuments(docs);
        writer.commit();

        docs.clear();
        document = new Document();
        document.add(createField("field2", 3));
        document.add(new StringField("filter_1", "T", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(createField("field2", 3));
        document.add(new StringField("filter_1", "T", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(createField("field2", 6));
        document.add(new StringField("filter_1", "T", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField("__type", "parent", Field.Store.NO));
        document.add(createField("field1", 6));
        docs.add(document);
        writer.addDocuments(docs);
        writer.commit();

        // This doc will not be included, because it doesn't have nested docs
        document = new Document();
        document.add(new StringField("__type", "parent", Field.Store.NO));
        document.add(createField("field1", 7));
        writer.addDocument(document);
        writer.commit();

        docs.clear();
        document = new Document();
        document.add(createField("field2", 3));
        document.add(new StringField("filter_1", "T", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(createField("field2", 3));
        document.add(new StringField("filter_1", "F", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(createField("field2", 7));
        document.add(new StringField("filter_1", "F", Field.Store.NO));
        docs.add(document);
        document = new Document();
        document.add(new StringField("__type", "parent", Field.Store.NO));
        document.add(createField("field1", 8));
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

        MultiValueMode sortMode = MultiValueMode.SUM;
        DirectoryReader directoryReader = DirectoryReader.open(writer);
        directoryReader = ElasticsearchDirectoryReader.wrap(directoryReader, new ShardId(indexService.index(), 0));
        IndexSearcher searcher = new IndexSearcher(directoryReader);
        Query parentFilter = new TermQuery(new Term("__type", "parent"));
        Query childFilter = Queries.not(parentFilter);
        XFieldComparatorSource nestedComparatorSource =
            createFieldComparator("field2", sortMode, null, createNested(searcher, parentFilter, childFilter));
        ToParentBlockJoinQuery query =
            new ToParentBlockJoinQuery(new ConstantScoreQuery(childFilter), new QueryBitSetProducer(parentFilter), ScoreMode.None);

        Sort sort = new Sort(new SortField("field2", nestedComparatorSource));
        TopFieldDocs topDocs = searcher.search(query, 5, sort);
        assertThat(topDocs.totalHits.value, equalTo(7L));
        assertThat(topDocs.scoreDocs.length, equalTo(5));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(11));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[0]).fields[0]).intValue(), equalTo(7));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(7));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[1]).fields[0]).intValue(), equalTo(8));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(3));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[2]).fields[0]).intValue(), equalTo(9));
        assertThat(topDocs.scoreDocs[3].doc, equalTo(15));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[3]).fields[0]).intValue(), equalTo(10));
        assertThat(topDocs.scoreDocs[4].doc, equalTo(19));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[4]).fields[0]).intValue(), equalTo(11));

        sort = new Sort(new SortField("field2", nestedComparatorSource, true));
        topDocs = searcher.search(query, 5, sort);
        assertThat(topDocs.totalHits.value, equalTo(7L));
        assertThat(topDocs.scoreDocs.length, equalTo(5));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(28));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[0]).fields[0]).intValue(), equalTo(13));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(23));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[1]).fields[0]).intValue(), equalTo(12));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(19));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[2]).fields[0]).intValue(), equalTo(11));
        assertThat(topDocs.scoreDocs[3].doc, equalTo(15));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[3]).fields[0]).intValue(), equalTo(10));
        assertThat(topDocs.scoreDocs[4].doc, equalTo(3));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[4]).fields[0]).intValue(), equalTo(9));

        childFilter = new TermQuery(new Term("filter_1", "T"));
        nestedComparatorSource =
            createFieldComparator("field2", sortMode, null, createNested(searcher, parentFilter, childFilter));
        query = new ToParentBlockJoinQuery(
                new ConstantScoreQuery(childFilter),
                new QueryBitSetProducer(parentFilter),
                ScoreMode.None
        );
        sort = new Sort(new SortField("field2", nestedComparatorSource, true));
        topDocs = searcher.search(query, 5, sort);
        assertThat(topDocs.totalHits.value, equalTo(6L));
        assertThat(topDocs.scoreDocs.length, equalTo(5));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(23));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[0]).fields[0]).intValue(), equalTo(12));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(3));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[1]).fields[0]).intValue(), equalTo(9));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(7));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[2]).fields[0]).intValue(), equalTo(8));
        assertThat(topDocs.scoreDocs[3].doc, equalTo(11));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[3]).fields[0]).intValue(), equalTo(7));
        assertThat(topDocs.scoreDocs[4].doc, equalTo(15));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[4]).fields[0]).intValue(), equalTo(3));

        sort = new Sort(new SortField("field2", nestedComparatorSource));
        topDocs = searcher.search(query, 5, sort);
        assertThat(topDocs.totalHits.value, equalTo(6L));
        assertThat(topDocs.scoreDocs.length, equalTo(5));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(15));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[0]).fields[0]).intValue(), equalTo(3));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(28));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[1]).fields[0]).intValue(), equalTo(3));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(11));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[2]).fields[0]).intValue(), equalTo(7));
        assertThat(topDocs.scoreDocs[3].doc, equalTo(7));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[3]).fields[0]).intValue(), equalTo(8));
        assertThat(topDocs.scoreDocs[4].doc, equalTo(3));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[4]).fields[0]).intValue(), equalTo(9));

        nestedComparatorSource =
            createFieldComparator("field2", sortMode, 127, createNested(searcher, parentFilter, childFilter));
        sort = new Sort(new SortField("field2", nestedComparatorSource, true));
        topDocs = searcher.search(new TermQuery(new Term("__type", "parent")), 5, sort);
        assertThat(topDocs.totalHits.value, equalTo(8L));
        assertThat(topDocs.scoreDocs.length, equalTo(5));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(19));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[0]).fields[0]).intValue(), equalTo(127));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(24));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[1]).fields[0]).intValue(), equalTo(127));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(23));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[2]).fields[0]).intValue(), equalTo(12));
        assertThat(topDocs.scoreDocs[3].doc, equalTo(3));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[3]).fields[0]).intValue(), equalTo(9));
        assertThat(topDocs.scoreDocs[4].doc, equalTo(7));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[4]).fields[0]).intValue(), equalTo(8));

        nestedComparatorSource =
            createFieldComparator("field2", sortMode, -127, createNested(searcher, parentFilter, childFilter));
        sort = new Sort(new SortField("field2", nestedComparatorSource));
        topDocs = searcher.search(new TermQuery(new Term("__type", "parent")), 5, sort);
        assertThat(topDocs.totalHits.value, equalTo(8L));
        assertThat(topDocs.scoreDocs.length, equalTo(5));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(19));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[0]).fields[0]).intValue(), equalTo(-127));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(24));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[1]).fields[0]).intValue(), equalTo(-127));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(15));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[2]).fields[0]).intValue(), equalTo(3));
        assertThat(topDocs.scoreDocs[3].doc, equalTo(28));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[3]).fields[0]).intValue(), equalTo(3));
        assertThat(topDocs.scoreDocs[4].doc, equalTo(11));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[4]).fields[0]).intValue(), equalTo(7));

        // Moved to method, because floating point based XFieldComparatorSource have different outcome for SortMode avg,
        // than integral number based implementations...
        assertAvgScoreMode(parentFilter, searcher);
        searcher.getIndexReader().close();
    }

    protected void assertAvgScoreMode(Query parentFilter, IndexSearcher searcher) throws IOException {
        MultiValueMode sortMode = MultiValueMode.AVG;
        Query childFilter = Queries.not(parentFilter);
        XFieldComparatorSource nestedComparatorSource =
            createFieldComparator("field2", sortMode, -127, createNested(searcher, parentFilter, childFilter));
        Query query =
            new ToParentBlockJoinQuery(new ConstantScoreQuery(childFilter), new QueryBitSetProducer(parentFilter), ScoreMode.None);
        Sort sort = new Sort(new SortField("field2", nestedComparatorSource));
        TopDocs topDocs = searcher.search(query, 5, sort);
        assertThat(topDocs.totalHits.value, equalTo(7L));
        assertThat(topDocs.scoreDocs.length, equalTo(5));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(11));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[0]).fields[0]).intValue(), equalTo(2));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(3));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[1]).fields[0]).intValue(), equalTo(3));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(7));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[2]).fields[0]).intValue(), equalTo(3));
        assertThat(topDocs.scoreDocs[3].doc, equalTo(15));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[3]).fields[0]).intValue(), equalTo(3));
        assertThat(topDocs.scoreDocs[4].doc, equalTo(19));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[4]).fields[0]).intValue(), equalTo(4));
    }

    protected abstract IndexableField createField(String name, int value);

    protected abstract IndexFieldData.XFieldComparatorSource createFieldComparator(String fieldName, MultiValueMode sortMode,
                                                                                        Object missingValue, Nested nested);

}
