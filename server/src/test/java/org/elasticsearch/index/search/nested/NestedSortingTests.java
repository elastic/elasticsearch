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
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
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
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.fielddata.AbstractFieldDataTestCase;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.NoOrdinalsStringFieldDataTests;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.index.fielddata.plain.PagedBytesIndexFieldData;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.NestedSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.index.mapper.SeqNoFieldMapper.PRIMARY_TERM_NAME;
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
                doc.add(new StringField("_nested_path", "child", Field.Store.NO));
                docs.add(doc);
            }
            if (randomBoolean()) {
                docs.add(new Document());
            }
            Document parent = new Document();
            parent.add(new StringField("_nested_path", "parent", Field.Store.NO));
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

    private TopDocs getTopDocs(
        IndexSearcher searcher,
        IndexFieldData<?> indexFieldData,
        String missingValue,
        MultiValueMode sortMode,
        int n,
        boolean reverse
    ) throws IOException {
        Query parentFilter = new TermQuery(new Term("_nested_path", "parent"));
        Query childFilter = new TermQuery(new Term("_nested_path", "child"));
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
        document.add(new StringField("_nested_path", "parent", Field.Store.NO));
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
        document.add(new StringField("_nested_path", "parent", Field.Store.NO));
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
        document.add(new StringField("_nested_path", "parent", Field.Store.NO));
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
        document.add(new StringField("_nested_path", "parent", Field.Store.NO));
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
        document.add(new StringField("_nested_path", "parent", Field.Store.NO));
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
        document.add(new StringField("_nested_path", "parent", Field.Store.NO));
        document.add(new StringField("field1", "g", Field.Store.NO));
        docs.add(document);
        writer.addDocuments(docs);

        // This doc will not be included, because it doesn't have nested docs
        document = new Document();
        document.add(new StringField("_nested_path", "parent", Field.Store.NO));
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
        document.add(new StringField("_nested_path", "parent", Field.Store.NO));
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
        Query parentFilter = new TermQuery(new Term("_nested_path", "parent"));
        Query childFilter = Queries.not(parentFilter);
        BytesRefFieldComparatorSource nestedComparatorSource = new BytesRefFieldComparatorSource(
            indexFieldData,
            null,
            sortMode,
            createNested(searcher, parentFilter, childFilter)
        );
        ToParentBlockJoinQuery query = new ToParentBlockJoinQuery(
            new ConstantScoreQuery(childFilter),
            new QueryBitSetProducer(parentFilter),
            ScoreMode.None
        );

        Sort sort = new Sort(new SortField("field2", nestedComparatorSource));
        TopFieldDocs topDocs = searcher.search(query, 5, sort);
        assertThat(topDocs.totalHits.value, equalTo(7L));
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
        nestedComparatorSource = new BytesRefFieldComparatorSource(
            indexFieldData,
            null,
            sortMode,
            createNested(searcher, parentFilter, childFilter)
        );
        sort = new Sort(new SortField("field2", nestedComparatorSource, true));
        topDocs = searcher.search(query, 5, sort);
        assertThat(topDocs.totalHits.value, equalTo(7L));
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
        nestedComparatorSource = new BytesRefFieldComparatorSource(
            indexFieldData,
            null,
            sortMode,
            createNested(searcher, parentFilter, childFilter)
        );
        query = new ToParentBlockJoinQuery(new ConstantScoreQuery(childFilter), new QueryBitSetProducer(parentFilter), ScoreMode.None);
        sort = new Sort(new SortField("field2", nestedComparatorSource, true));
        topDocs = searcher.search(query, 5, sort);
        assertThat(topDocs.totalHits.value, equalTo(6L));
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

    public void testMultiLevelNestedSorting() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        {
            mapping.startObject("_doc");
            {
                mapping.startObject("properties");
                {
                    {
                        mapping.startObject("title");
                        mapping.field("type", "text");
                        mapping.endObject();
                    }
                    {
                        mapping.startObject("genre");
                        mapping.field("type", "keyword");
                        mapping.endObject();
                    }
                    {
                        mapping.startObject("chapters");
                        mapping.field("type", "nested");
                        {
                            mapping.startObject("properties");
                            {
                                mapping.startObject("title");
                                mapping.field("type", "text");
                                mapping.endObject();
                            }
                            {
                                mapping.startObject("read_time_seconds");
                                mapping.field("type", "integer");
                                mapping.endObject();
                            }
                            {
                                mapping.startObject("paragraphs");
                                mapping.field("type", "nested");
                                {
                                    mapping.startObject("properties");
                                    {
                                        {
                                            mapping.startObject("header");
                                            mapping.field("type", "text");
                                            mapping.endObject();
                                        }
                                        {
                                            mapping.startObject("content");
                                            mapping.field("type", "text");
                                            mapping.endObject();
                                        }
                                        {
                                            mapping.startObject("word_count");
                                            mapping.field("type", "integer");
                                            mapping.endObject();
                                        }
                                    }
                                    mapping.endObject();
                                }
                                mapping.endObject();
                            }
                            mapping.endObject();
                        }
                        mapping.endObject();
                    }
                }
                mapping.endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();
        IndexService indexService = createIndex("nested_sorting", Settings.EMPTY, mapping);

        List<List<Document>> books = new ArrayList<>();
        {
            List<Document> book = new ArrayList<>();
            Document document = new Document();
            document.add(new TextField("chapters.paragraphs.header", "Paragraph 1", Field.Store.NO));
            document.add(new StringField("_nested_path", "chapters.paragraphs", Field.Store.NO));
            document.add(new TextField("chapters.paragraphs.text", "some text...", Field.Store.NO));
            document.add(new SortedNumericDocValuesField("chapters.paragraphs.word_count", 743));
            document.add(new IntPoint("chapters.paragraphs.word_count", 743));
            book.add(document);
            document = new Document();
            document.add(new TextField("chapters.title", "chapter 3", Field.Store.NO));
            document.add(new StringField("_nested_path", "chapters", Field.Store.NO));
            document.add(new IntPoint("chapters.read_time_seconds", 400));
            document.add(new NumericDocValuesField("chapters.read_time_seconds", 400));
            book.add(document);
            document = new Document();
            document.add(new TextField("chapters.paragraphs.header", "Paragraph 1", Field.Store.NO));
            document.add(new StringField("_nested_path", "chapters.paragraphs", Field.Store.NO));
            document.add(new TextField("chapters.paragraphs.text", "some text...", Field.Store.NO));
            document.add(new SortedNumericDocValuesField("chapters.paragraphs.word_count", 234));
            document.add(new IntPoint("chapters.paragraphs.word_count", 234));
            book.add(document);
            document = new Document();
            document.add(new TextField("chapters.title", "chapter 2", Field.Store.NO));
            document.add(new StringField("_nested_path", "chapters", Field.Store.NO));
            document.add(new IntPoint("chapters.read_time_seconds", 200));
            document.add(new NumericDocValuesField("chapters.read_time_seconds", 200));
            book.add(document);
            document = new Document();
            document.add(new TextField("chapters.paragraphs.header", "Paragraph 2", Field.Store.NO));
            document.add(new StringField("_nested_path", "chapters.paragraphs", Field.Store.NO));
            document.add(new TextField("chapters.paragraphs.text", "some text...", Field.Store.NO));
            document.add(new SortedNumericDocValuesField("chapters.paragraphs.word_count", 478));
            document.add(new IntPoint("chapters.paragraphs.word_count", 478));
            book.add(document);
            document = new Document();
            document.add(new TextField("chapters.paragraphs.header", "Paragraph 1", Field.Store.NO));
            document.add(new StringField("_nested_path", "chapters.paragraphs", Field.Store.NO));
            document.add(new TextField("chapters.paragraphs.text", "some text...", Field.Store.NO));
            document.add(new SortedNumericDocValuesField("chapters.paragraphs.word_count", 849));
            document.add(new IntPoint("chapters.paragraphs.word_count", 849));
            book.add(document);
            document = new Document();
            document.add(new TextField("chapters.title", "chapter 1", Field.Store.NO));
            document.add(new StringField("_nested_path", "chapters", Field.Store.NO));
            document.add(new IntPoint("chapters.read_time_seconds", 1400));
            document.add(new NumericDocValuesField("chapters.read_time_seconds", 1400));
            book.add(document);
            document = new Document();
            document.add(new StringField("genre", "science fiction", Field.Store.NO));
            document.add(new StringField("_id", "1", Field.Store.YES));
            document.add(new NumericDocValuesField(PRIMARY_TERM_NAME, 0));
            book.add(document);
            books.add(book);
        }
        {
            List<Document> book = new ArrayList<>();
            Document document = new Document();
            document.add(new TextField("chapters.paragraphs.header", "Introduction", Field.Store.NO));
            document.add(new StringField("_nested_path", "chapters.paragraphs", Field.Store.NO));
            document.add(new TextField("chapters.paragraphs.text", "some text...", Field.Store.NO));
            document.add(new SortedNumericDocValuesField("chapters.paragraphs.word_count", 76));
            document.add(new IntPoint("chapters.paragraphs.word_count", 76));
            book.add(document);
            document = new Document();
            document.add(new TextField("chapters.title", "chapter 1", Field.Store.NO));
            document.add(new StringField("_nested_path", "chapters", Field.Store.NO));
            document.add(new IntPoint("chapters.read_time_seconds", 20));
            document.add(new NumericDocValuesField("chapters.read_time_seconds", 20));
            book.add(document);
            document = new Document();
            document.add(new StringField("genre", "romance", Field.Store.NO));
            document.add(new StringField("_id", "2", Field.Store.YES));
            document.add(new NumericDocValuesField(PRIMARY_TERM_NAME, 0));
            book.add(document);
            books.add(book);
        }
        {
            List<Document> book = new ArrayList<>();
            Document document = new Document();
            document.add(new TextField("chapters.paragraphs.header", "A bad dream", Field.Store.NO));
            document.add(new StringField("_nested_path", "chapters.paragraphs", Field.Store.NO));
            document.add(new TextField("chapters.paragraphs.text", "some text...", Field.Store.NO));
            document.add(new SortedNumericDocValuesField("chapters.paragraphs.word_count", 976));
            document.add(new IntPoint("chapters.paragraphs.word_count", 976));
            book.add(document);
            document = new Document();
            document.add(new TextField("chapters.title", "The beginning of the end", Field.Store.NO));
            document.add(new StringField("_nested_path", "chapters", Field.Store.NO));
            document.add(new IntPoint("chapters.read_time_seconds", 1200));
            document.add(new NumericDocValuesField("chapters.read_time_seconds", 1200));
            book.add(document);
            document = new Document();
            document.add(new StringField("genre", "horror", Field.Store.NO));
            document.add(new StringField("_id", "3", Field.Store.YES));
            document.add(new NumericDocValuesField(PRIMARY_TERM_NAME, 0));
            book.add(document);
            books.add(book);
        }
        {
            List<Document> book = new ArrayList<>();
            Document document = new Document();
            document.add(new TextField("chapters.paragraphs.header", "macaroni", Field.Store.NO));
            document.add(new StringField("_nested_path", "chapters.paragraphs", Field.Store.NO));
            document.add(new TextField("chapters.paragraphs.text", "some text...", Field.Store.NO));
            document.add(new SortedNumericDocValuesField("chapters.paragraphs.word_count", 180));
            document.add(new IntPoint("chapters.paragraphs.word_count", 180));
            book.add(document);
            document = new Document();
            document.add(new TextField("chapters.paragraphs.header", "hamburger", Field.Store.NO));
            document.add(new StringField("_nested_path", "chapters.paragraphs", Field.Store.NO));
            document.add(new TextField("chapters.paragraphs.text", "some text...", Field.Store.NO));
            document.add(new SortedNumericDocValuesField("chapters.paragraphs.word_count", 150));
            document.add(new IntPoint("chapters.paragraphs.word_count", 150));
            book.add(document);
            document = new Document();
            document.add(new TextField("chapters.paragraphs.header", "tosti", Field.Store.NO));
            document.add(new StringField("_nested_path", "chapters.paragraphs", Field.Store.NO));
            document.add(new TextField("chapters.paragraphs.text", "some text...", Field.Store.NO));
            document.add(new SortedNumericDocValuesField("chapters.paragraphs.word_count", 120));
            document.add(new IntPoint("chapters.paragraphs.word_count", 120));
            book.add(document);
            document = new Document();
            document.add(new TextField("chapters.title", "easy meals", Field.Store.NO));
            document.add(new StringField("_nested_path", "chapters", Field.Store.NO));
            document.add(new IntPoint("chapters.read_time_seconds", 800));
            document.add(new NumericDocValuesField("chapters.read_time_seconds", 800));
            book.add(document);
            document = new Document();
            document.add(new TextField("chapters.paragraphs.header", "introduction", Field.Store.NO));
            document.add(new StringField("_nested_path", "chapters.paragraphs", Field.Store.NO));
            document.add(new TextField("chapters.paragraphs.text", "some text...", Field.Store.NO));
            document.add(new SortedNumericDocValuesField("chapters.paragraphs.word_count", 87));
            document.add(new IntPoint("chapters.paragraphs.word_count", 87));
            book.add(document);
            document = new Document();
            document.add(new TextField("chapters.title", "introduction", Field.Store.NO));
            document.add(new StringField("_nested_path", "chapters", Field.Store.NO));
            document.add(new IntPoint("chapters.read_time_seconds", 10));
            document.add(new NumericDocValuesField("chapters.read_time_seconds", 10));
            book.add(document);
            document = new Document();
            document.add(new StringField("genre", "cooking", Field.Store.NO));
            document.add(new StringField("_id", "4", Field.Store.YES));
            document.add(new NumericDocValuesField(PRIMARY_TERM_NAME, 0));
            book.add(document);
            books.add(book);
        }
        {
            List<Document> book = new ArrayList<>();
            Document document = new Document();
            document.add(new StringField("genre", "unknown", Field.Store.NO));
            document.add(new StringField("_id", "5", Field.Store.YES));
            document.add(new NumericDocValuesField(PRIMARY_TERM_NAME, 0));
            book.add(document);
            books.add(book);
        }

        Collections.shuffle(books, random());
        for (List<Document> book : books) {
            writer.addDocuments(book);
            if (randomBoolean()) {
                writer.commit();
            }
        }
        DirectoryReader reader = DirectoryReader.open(writer);
        reader = ElasticsearchDirectoryReader.wrap(reader, new ShardId(indexService.index(), 0));
        IndexSearcher searcher = new IndexSearcher(reader);
        SearchExecutionContext searchExecutionContext = indexService.newSearchExecutionContext(0, 0, searcher, () -> 0L, null, emptyMap());

        FieldSortBuilder sortBuilder = new FieldSortBuilder("chapters.paragraphs.word_count");
        sortBuilder.setNestedSort(new NestedSortBuilder("chapters").setNestedSort(new NestedSortBuilder("chapters.paragraphs")));
        QueryBuilder queryBuilder = new MatchAllQueryBuilder();
        TopFieldDocs topFields = search(queryBuilder, sortBuilder, searchExecutionContext, searcher);
        assertThat(topFields.totalHits.value, equalTo(5L));
        assertThat(searcher.doc(topFields.scoreDocs[0].doc).get("_id"), equalTo("2"));
        assertThat(((FieldDoc) topFields.scoreDocs[0]).fields[0], equalTo(76L));
        assertThat(searcher.doc(topFields.scoreDocs[1].doc).get("_id"), equalTo("4"));
        assertThat(((FieldDoc) topFields.scoreDocs[1]).fields[0], equalTo(87L));
        assertThat(searcher.doc(topFields.scoreDocs[2].doc).get("_id"), equalTo("1"));
        assertThat(((FieldDoc) topFields.scoreDocs[2]).fields[0], equalTo(234L));
        assertThat(searcher.doc(topFields.scoreDocs[3].doc).get("_id"), equalTo("3"));
        assertThat(((FieldDoc) topFields.scoreDocs[3]).fields[0], equalTo(976L));
        assertThat(searcher.doc(topFields.scoreDocs[4].doc).get("_id"), equalTo("5"));
        assertThat(((FieldDoc) topFields.scoreDocs[4]).fields[0], equalTo(Long.MAX_VALUE));

        // Specific genre
        {
            queryBuilder = new TermQueryBuilder("genre", "romance");
            topFields = search(queryBuilder, sortBuilder, searchExecutionContext, searcher);
            assertThat(topFields.totalHits.value, equalTo(1L));
            assertThat(searcher.doc(topFields.scoreDocs[0].doc).get("_id"), equalTo("2"));
            assertThat(((FieldDoc) topFields.scoreDocs[0]).fields[0], equalTo(76L));

            queryBuilder = new TermQueryBuilder("genre", "science fiction");
            topFields = search(queryBuilder, sortBuilder, searchExecutionContext, searcher);
            assertThat(topFields.totalHits.value, equalTo(1L));
            assertThat(searcher.doc(topFields.scoreDocs[0].doc).get("_id"), equalTo("1"));
            assertThat(((FieldDoc) topFields.scoreDocs[0]).fields[0], equalTo(234L));

            queryBuilder = new TermQueryBuilder("genre", "horror");
            topFields = search(queryBuilder, sortBuilder, searchExecutionContext, searcher);
            assertThat(topFields.totalHits.value, equalTo(1L));
            assertThat(searcher.doc(topFields.scoreDocs[0].doc).get("_id"), equalTo("3"));
            assertThat(((FieldDoc) topFields.scoreDocs[0]).fields[0], equalTo(976L));

            queryBuilder = new TermQueryBuilder("genre", "cooking");
            topFields = search(queryBuilder, sortBuilder, searchExecutionContext, searcher);
            assertThat(topFields.totalHits.value, equalTo(1L));
            assertThat(searcher.doc(topFields.scoreDocs[0].doc).get("_id"), equalTo("4"));
            assertThat(((FieldDoc) topFields.scoreDocs[0]).fields[0], equalTo(87L));
        }

        // reverse sort order
        {
            sortBuilder.order(SortOrder.DESC);
            queryBuilder = new MatchAllQueryBuilder();
            topFields = search(queryBuilder, sortBuilder, searchExecutionContext, searcher);
            assertThat(topFields.totalHits.value, equalTo(5L));
            assertThat(searcher.doc(topFields.scoreDocs[0].doc).get("_id"), equalTo("3"));
            assertThat(((FieldDoc) topFields.scoreDocs[0]).fields[0], equalTo(976L));
            assertThat(searcher.doc(topFields.scoreDocs[1].doc).get("_id"), equalTo("1"));
            assertThat(((FieldDoc) topFields.scoreDocs[1]).fields[0], equalTo(849L));
            assertThat(searcher.doc(topFields.scoreDocs[2].doc).get("_id"), equalTo("4"));
            assertThat(((FieldDoc) topFields.scoreDocs[2]).fields[0], equalTo(180L));
            assertThat(searcher.doc(topFields.scoreDocs[3].doc).get("_id"), equalTo("2"));
            assertThat(((FieldDoc) topFields.scoreDocs[3]).fields[0], equalTo(76L));
            assertThat(searcher.doc(topFields.scoreDocs[4].doc).get("_id"), equalTo("5"));
            assertThat(((FieldDoc) topFields.scoreDocs[4]).fields[0], equalTo(Long.MIN_VALUE));
        }

        // Specific genre and reverse sort order
        {
            queryBuilder = new TermQueryBuilder("genre", "romance");
            topFields = search(queryBuilder, sortBuilder, searchExecutionContext, searcher);
            assertThat(topFields.totalHits.value, equalTo(1L));
            assertThat(searcher.doc(topFields.scoreDocs[0].doc).get("_id"), equalTo("2"));
            assertThat(((FieldDoc) topFields.scoreDocs[0]).fields[0], equalTo(76L));

            queryBuilder = new TermQueryBuilder("genre", "science fiction");
            topFields = search(queryBuilder, sortBuilder, searchExecutionContext, searcher);
            assertThat(topFields.totalHits.value, equalTo(1L));
            assertThat(searcher.doc(topFields.scoreDocs[0].doc).get("_id"), equalTo("1"));
            assertThat(((FieldDoc) topFields.scoreDocs[0]).fields[0], equalTo(849L));

            queryBuilder = new TermQueryBuilder("genre", "horror");
            topFields = search(queryBuilder, sortBuilder, searchExecutionContext, searcher);
            assertThat(topFields.totalHits.value, equalTo(1L));
            assertThat(searcher.doc(topFields.scoreDocs[0].doc).get("_id"), equalTo("3"));
            assertThat(((FieldDoc) topFields.scoreDocs[0]).fields[0], equalTo(976L));

            queryBuilder = new TermQueryBuilder("genre", "cooking");
            topFields = search(queryBuilder, sortBuilder, searchExecutionContext, searcher);
            assertThat(topFields.totalHits.value, equalTo(1L));
            assertThat(searcher.doc(topFields.scoreDocs[0].doc).get("_id"), equalTo("4"));
            assertThat(((FieldDoc) topFields.scoreDocs[0]).fields[0], equalTo(180L));
        }

        // Nested filter + query
        {
            queryBuilder = new RangeQueryBuilder("chapters.read_time_seconds").to(50L);
            sortBuilder = new FieldSortBuilder("chapters.paragraphs.word_count");
            sortBuilder.setNestedSort(
                new NestedSortBuilder("chapters").setFilter(queryBuilder).setNestedSort(new NestedSortBuilder("chapters.paragraphs"))
            );
            topFields = search(
                new NestedQueryBuilder("chapters", queryBuilder, ScoreMode.None),
                sortBuilder,
                searchExecutionContext,
                searcher
            );
            assertThat(topFields.totalHits.value, equalTo(2L));
            assertThat(searcher.doc(topFields.scoreDocs[0].doc).get("_id"), equalTo("2"));
            assertThat(((FieldDoc) topFields.scoreDocs[0]).fields[0], equalTo(76L));
            assertThat(searcher.doc(topFields.scoreDocs[1].doc).get("_id"), equalTo("4"));
            assertThat(((FieldDoc) topFields.scoreDocs[1]).fields[0], equalTo(87L));

            sortBuilder.order(SortOrder.DESC);
            topFields = search(
                new NestedQueryBuilder("chapters", queryBuilder, ScoreMode.None),
                sortBuilder,
                searchExecutionContext,
                searcher
            );
            assertThat(topFields.totalHits.value, equalTo(2L));
            assertThat(searcher.doc(topFields.scoreDocs[0].doc).get("_id"), equalTo("4"));
            assertThat(((FieldDoc) topFields.scoreDocs[0]).fields[0], equalTo(87L));
            assertThat(searcher.doc(topFields.scoreDocs[1].doc).get("_id"), equalTo("2"));
            assertThat(((FieldDoc) topFields.scoreDocs[1]).fields[0], equalTo(76L));
        }

        // Multiple Nested filters + query
        {
            queryBuilder = new RangeQueryBuilder("chapters.read_time_seconds").to(50L);
            sortBuilder = new FieldSortBuilder("chapters.paragraphs.word_count");
            sortBuilder.setNestedSort(
                new NestedSortBuilder("chapters").setFilter(queryBuilder)
                    .setNestedSort(
                        new NestedSortBuilder("chapters.paragraphs").setFilter(
                            new RangeQueryBuilder("chapters.paragraphs.word_count").from(80L)
                        )
                    )
            );
            topFields = search(
                new NestedQueryBuilder("chapters", queryBuilder, ScoreMode.None),
                sortBuilder,
                searchExecutionContext,
                searcher
            );
            assertThat(topFields.totalHits.value, equalTo(2L));
            assertThat(searcher.doc(topFields.scoreDocs[0].doc).get("_id"), equalTo("4"));
            assertThat(((FieldDoc) topFields.scoreDocs[0]).fields[0], equalTo(87L));
            assertThat(searcher.doc(topFields.scoreDocs[1].doc).get("_id"), equalTo("2"));
            assertThat(((FieldDoc) topFields.scoreDocs[1]).fields[0], equalTo(Long.MAX_VALUE));

            sortBuilder.order(SortOrder.DESC);
            topFields = search(
                new NestedQueryBuilder("chapters", queryBuilder, ScoreMode.None),
                sortBuilder,
                searchExecutionContext,
                searcher
            );
            assertThat(topFields.totalHits.value, equalTo(2L));
            assertThat(searcher.doc(topFields.scoreDocs[0].doc).get("_id"), equalTo("4"));
            assertThat(((FieldDoc) topFields.scoreDocs[0]).fields[0], equalTo(87L));
            assertThat(searcher.doc(topFields.scoreDocs[1].doc).get("_id"), equalTo("2"));
            assertThat(((FieldDoc) topFields.scoreDocs[1]).fields[0], equalTo(Long.MIN_VALUE));
        }

        // Nested filter + Specific genre
        {
            sortBuilder = new FieldSortBuilder("chapters.paragraphs.word_count");
            sortBuilder.setNestedSort(
                new NestedSortBuilder("chapters").setFilter(new RangeQueryBuilder("chapters.read_time_seconds").to(50L))
                    .setNestedSort(new NestedSortBuilder("chapters.paragraphs"))
            );

            queryBuilder = new TermQueryBuilder("genre", "romance");
            topFields = search(queryBuilder, sortBuilder, searchExecutionContext, searcher);
            assertThat(topFields.totalHits.value, equalTo(1L));
            assertThat(searcher.doc(topFields.scoreDocs[0].doc).get("_id"), equalTo("2"));
            assertThat(((FieldDoc) topFields.scoreDocs[0]).fields[0], equalTo(76L));

            queryBuilder = new TermQueryBuilder("genre", "science fiction");
            topFields = search(queryBuilder, sortBuilder, searchExecutionContext, searcher);
            assertThat(topFields.totalHits.value, equalTo(1L));
            assertThat(searcher.doc(topFields.scoreDocs[0].doc).get("_id"), equalTo("1"));
            assertThat(((FieldDoc) topFields.scoreDocs[0]).fields[0], equalTo(Long.MAX_VALUE));

            queryBuilder = new TermQueryBuilder("genre", "horror");
            topFields = search(queryBuilder, sortBuilder, searchExecutionContext, searcher);
            assertThat(topFields.totalHits.value, equalTo(1L));
            assertThat(searcher.doc(topFields.scoreDocs[0].doc).get("_id"), equalTo("3"));
            assertThat(((FieldDoc) topFields.scoreDocs[0]).fields[0], equalTo(Long.MAX_VALUE));

            queryBuilder = new TermQueryBuilder("genre", "cooking");
            topFields = search(queryBuilder, sortBuilder, searchExecutionContext, searcher);
            assertThat(topFields.totalHits.value, equalTo(1L));
            assertThat(searcher.doc(topFields.scoreDocs[0].doc).get("_id"), equalTo("4"));
            assertThat(((FieldDoc) topFields.scoreDocs[0]).fields[0], equalTo(87L));
        }

        searcher.getIndexReader().close();
    }

    private static TopFieldDocs search(
        QueryBuilder queryBuilder,
        FieldSortBuilder sortBuilder,
        SearchExecutionContext searchExecutionContext,
        IndexSearcher searcher
    ) throws IOException {
        Query query = new BooleanQuery.Builder().add(queryBuilder.toQuery(searchExecutionContext), Occur.MUST)
            .add(Queries.newNonNestedFilter(searchExecutionContext.indexVersionCreated()), Occur.FILTER)
            .build();
        Sort sort = new Sort(sortBuilder.build(searchExecutionContext).field);
        return searcher.search(query, 10, sort);
    }

}
