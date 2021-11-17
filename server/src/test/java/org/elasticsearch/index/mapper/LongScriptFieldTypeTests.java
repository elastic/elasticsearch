/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.lucene.search.function.ScriptScoreQuery;
import org.elasticsearch.index.fielddata.LongScriptFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.queryableexpression.QueryableExpressionBuilder;
import org.elasticsearch.script.DocReader;
import org.elasticsearch.script.LongFieldScript;
import org.elasticsearch.script.LongFieldScript.LeafFactory;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.runtime.AbstractScriptFieldQuery;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

public class LongScriptFieldTypeTests extends AbstractNonTextScriptFieldTypeTestCase {

    public void testFormat() throws IOException {
        assertThat(simpleMappedFieldType().docValueFormat("#.0", null).format(1), equalTo("1.0"));
        assertThat(simpleMappedFieldType().docValueFormat("#,##0.##", null).format(11), equalTo("11"));
        assertThat(simpleMappedFieldType().docValueFormat("#,##0.##", null).format(1123), equalTo("1,123"));
    }

    public void testLongFromSource() throws IOException {
        MapperService mapperService = createMapperService(runtimeFieldMapping(b -> b.field("type", "long")));
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field("field", "9223372036854775806.00")));
        withLuceneIndex(mapperService, iw -> iw.addDocuments(doc.docs()), ir -> {
            MappedFieldType ft = mapperService.fieldType("field");
            SearchExecutionContext sec = createSearchExecutionContext(mapperService);
            Query rangeQuery = ft.rangeQuery(0, 9223372036854775807L, false, false, ShapeRelation.CONTAINS, null, null, sec);
            IndexSearcher searcher = new IndexSearcher(ir);
            assertEquals(1, searcher.count(rangeQuery));
        });
    }

    @Override
    public void testDocValues() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [1]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [2, 1]}"))));
            List<Long> results = new ArrayList<>();
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                LongScriptFieldType ft = build("add_param", Map.of("param", 1), randomBoolean());
                LongScriptFieldData ifd = ft.fielddataBuilder("test", mockContext()::lookup).build(null, null);
                searcher.search(new MatchAllDocsQuery(), new Collector() {
                    @Override
                    public ScoreMode scoreMode() {
                        return ScoreMode.COMPLETE_NO_SCORES;
                    }

                    @Override
                    public LeafCollector getLeafCollector(LeafReaderContext context) {
                        SortedNumericDocValues dv = ifd.load(context).getLongValues();
                        return new LeafCollector() {
                            @Override
                            public void setScorer(Scorable scorer) {}

                            @Override
                            public void collect(int doc) throws IOException {
                                if (dv.advanceExact(doc)) {
                                    for (int i = 0; i < dv.docValueCount(); i++) {
                                        results.add(dv.nextValue());
                                    }
                                }
                            }
                        };
                    }
                });
                assertThat(results, equalTo(List.of(2L, 2L, 3L)));
            }
        }
    }

    @Override
    public void testSort() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [1]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [4]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [2]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                LongScriptFieldData ifd = simpleMappedFieldType().fielddataBuilder("test", mockContext()::lookup).build(null, null);
                SortField sf = ifd.sortField(null, MultiValueMode.MIN, null, false);
                TopFieldDocs docs = searcher.search(new MatchAllDocsQuery(), 3, new Sort(sf));
                assertThat(reader.document(docs.scoreDocs[0].doc).getBinaryValue("_source").utf8ToString(), equalTo("{\"foo\": [1]}"));
                assertThat(reader.document(docs.scoreDocs[1].doc).getBinaryValue("_source").utf8ToString(), equalTo("{\"foo\": [2]}"));
                assertThat(reader.document(docs.scoreDocs[2].doc).getBinaryValue("_source").utf8ToString(), equalTo("{\"foo\": [4]}"));
            }
        }
    }

    public void testNow() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"timestamp\": [1595432181354]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"timestamp\": [1595432181351]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"timestamp\": [1595432181356]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                LongScriptFieldData ifd = build("millis_ago", Map.of(), randomBoolean()).fielddataBuilder("test", mockContext()::lookup)
                    .build(null, null);
                SortField sf = ifd.sortField(null, MultiValueMode.MIN, null, false);
                TopFieldDocs docs = searcher.search(new MatchAllDocsQuery(), 3, new Sort(sf));
                assertThat(readSource(reader, docs.scoreDocs[0].doc), equalTo("{\"timestamp\": [1595432181356]}"));
                assertThat(readSource(reader, docs.scoreDocs[1].doc), equalTo("{\"timestamp\": [1595432181354]}"));
                assertThat(readSource(reader, docs.scoreDocs[2].doc), equalTo("{\"timestamp\": [1595432181351]}"));
                long t1 = (Long) (((FieldDoc) docs.scoreDocs[0]).fields[0]);
                assertThat(t1, greaterThan(3638011399L));
                long t2 = (Long) (((FieldDoc) docs.scoreDocs[1]).fields[0]);
                long t3 = (Long) (((FieldDoc) docs.scoreDocs[2]).fields[0]);
                assertThat(t2, equalTo(t1 + 2));
                assertThat(t3, equalTo(t1 + 5));
            }
        }
    }

    @Override
    public void testUsedInScript() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [1]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [4]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [2]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                SearchExecutionContext searchContext = mockContext(true, simpleMappedFieldType());
                assertThat(searcher.count(new ScriptScoreQuery(new MatchAllDocsQuery(), new Script("test"), new ScoreScript.LeafFactory() {
                    @Override
                    public boolean needs_score() {
                        return false;
                    }

                    @Override
                    public ScoreScript newInstance(DocReader docReader) {
                        return new ScoreScript(Map.of(), searchContext.lookup(), docReader) {
                            @Override
                            public double execute(ExplanationHolder explanation) {
                                ScriptDocValues.Longs longs = (ScriptDocValues.Longs) getDoc().get("test");
                                return longs.get(0);
                            }
                        };
                    }
                }, searchContext.lookup(), 2.5f, "test", 0, Version.CURRENT)), equalTo(1));
            }
        }
    }

    @Override
    public void testExistsQuery() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [1]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": []}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                assertThat(searcher.count(simpleMappedFieldType().existsQuery(mockContext())), equalTo(1));
            }
        }
    }

    @Override
    public void testRangeQuery() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [1]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [2]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                MappedFieldType ft = simpleMappedFieldType();
                assertThat(searcher.count(ft.rangeQuery("2", "3", true, true, null, null, null, mockContext())), equalTo(1));
                assertThat(searcher.count(ft.rangeQuery(2, 3, true, true, null, null, null, mockContext())), equalTo(1));
                assertThat(searcher.count(ft.rangeQuery(1.1, 3, true, true, null, null, null, mockContext())), equalTo(1));
                assertThat(searcher.count(ft.rangeQuery(1.1, 3, false, true, null, null, null, mockContext())), equalTo(1));
                assertThat(searcher.count(ft.rangeQuery(2, 3, false, true, null, null, null, mockContext())), equalTo(0));
                assertThat(ft.rangeQuery(3, 2, false, false, null, null, null, mockContext()), instanceOf(MatchNoDocsQuery.class));
            }
        }
    }

    @Override
    protected Query randomRangeQuery(MappedFieldType ft, SearchExecutionContext ctx) {
        long a = randomLong();
        long b = randomLong();
        return ft.rangeQuery(Math.min(a, b), Math.max(a, b), randomBoolean(), randomBoolean(), null, null, null, ctx);
    }

    @Override
    public void testTermQuery() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [1]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [2]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                assertThat(searcher.count(simpleMappedFieldType().termQuery("1", mockContext())), equalTo(1));
                assertThat(searcher.count(simpleMappedFieldType().termQuery(1, mockContext())), equalTo(1));
                assertThat(searcher.count(simpleMappedFieldType().termQuery(1.1, mockContext())), equalTo(0));
                assertThat(searcher.count(build("add_param", Map.of("param", 1), randomBoolean()).termQuery(2, mockContext())), equalTo(1));
            }
        }
    }

    public void testTermWithApproximationDisabled() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", 1), new LongPoint("foo", 1)));
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", 2), new LongPoint("foo", 2)));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                SearchExecutionContext context = mockContext(true, new NumberFieldMapper.NumberFieldType("foo", NumberType.LONG));
                MappedFieldType ft = build("foo_plus_five", Map.of(), false);
                assertCountAndApproximation(searcher, ft.termQuery(6, context), equalTo(1), new MatchAllDocsQuery());
            }
        }
    }

    private Query expectedTermApproximationFooLongTimesTen(Query precise) {
        return should(
            precise,
            longRangeQuery("foo", Long.MAX_VALUE / 10, Long.MAX_VALUE),
            longRangeQuery("foo", Long.MIN_VALUE, Long.MIN_VALUE / 10)
        );
    }

    private Query expectedTermApproximationFooLongTimesNegativeTwo(Query precise) {
        return should(
            precise,
            longRangeQuery("foo", Long.MIN_VALUE / -2, Long.MAX_VALUE),
            longRangeQuery("foo", Long.MIN_VALUE, Long.MAX_VALUE / -2)
        );
    }

    public void testTermWithApproximationFromLong() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", 1), new LongPoint("foo", 1)));
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", 2), new LongPoint("foo", 2)));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                SearchExecutionContext context = mockContext(true, new NumberFieldMapper.NumberFieldType("foo", NumberType.LONG));
                MappedFieldType ft = build("foo_plus_five", Map.of(), true);
                assertCountAndApproximation(searcher, ft.termQuery(6, context), equalTo(1), LongPoint.newExactQuery("foo", 1));
                assertCountAndQuery(
                    searcher,
                    ft.termQuery(6.1, context),
                    equalTo(0),
                    new MatchNoDocsQuery("Value [6.1] has a decimal part")
                );
                assertCountAndApproximation(searcher, ft.termQuery(7, context), equalTo(1), LongPoint.newExactQuery("foo", 2));
                assertCountAndApproximation(searcher, ft.termQuery(8, context), equalTo(0), LongPoint.newExactQuery("foo", 3));

                ft = build("foo_times_ten", Map.of(), true);
                assertCountAndApproximation(
                    searcher,
                    ft.termQuery(10, context),
                    equalTo(1),
                    expectedTermApproximationFooLongTimesTen(LongPoint.newExactQuery("foo", 1))
                );
                assertCountAndQuery(
                    searcher,
                    ft.termQuery(10.1, context),
                    equalTo(0),
                    new MatchNoDocsQuery("Value [10.1] has a decimal part")
                );
                assertCountAndApproximation(
                    searcher,
                    ft.termQuery(20, context),
                    equalTo(1),
                    expectedTermApproximationFooLongTimesTen(LongPoint.newExactQuery("foo", 2))
                );
                assertCountAndApproximation(
                    searcher,
                    ft.termQuery(30, context),
                    equalTo(0),
                    expectedTermApproximationFooLongTimesTen(LongPoint.newExactQuery("foo", 3))
                );

                ft = build("foo_times_negative_two", Map.of(), true);
                assertCountAndApproximation(
                    searcher,
                    ft.termQuery(-2, context),
                    equalTo(1),
                    expectedTermApproximationFooLongTimesNegativeTwo(LongPoint.newExactQuery("foo", 1))
                );
                assertCountAndApproximation(
                    searcher,
                    ft.termQuery(-4, context),
                    equalTo(1),
                    expectedTermApproximationFooLongTimesNegativeTwo(LongPoint.newExactQuery("foo", 2))
                );

                ft = build("foo_divided_by_ten", Map.of(), true);
                assertCountAndApproximation(searcher, ft.termQuery(0, context), equalTo(2), longRangeQuery("foo", -9, 9));
                assertCountAndApproximation(searcher, ft.termQuery(1, context), equalTo(0), longRangeQuery("foo", 10, 19));

                ft = build("ten_over_foo", Map.of(), true);
                assertCountAndApproximation(searcher, ft.termQuery(10, context), equalTo(1), new DocValuesFieldExistsQuery("foo"));
                assertCountAndApproximation(searcher, ft.termQuery(5, context), equalTo(1), new DocValuesFieldExistsQuery("foo"));
                assertCountAndApproximation(searcher, ft.termQuery(0, context), equalTo(0), new DocValuesFieldExistsQuery("foo"));
            }
        }
    }

    public void testTermNearMaxIntWithApproximationFromLong() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", Long.MAX_VALUE - 1), new LongPoint("foo", Long.MAX_VALUE - 1)));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                SearchExecutionContext context = mockContext(true, new NumberFieldMapper.NumberFieldType("foo", NumberType.LONG));
                MappedFieldType ft = build("foo_plus_five", Map.of(), true);
                assertCountAndApproximation(
                    searcher,
                    ft.termQuery(Long.MAX_VALUE + 4, context),
                    equalTo(1),
                    LongPoint.newExactQuery("foo", Long.MAX_VALUE - 1)
                );

                ft = build("foo_times_ten", Map.of(), true);
                assertCountAndApproximation(
                    searcher,
                    ft.termQuery((Long.MAX_VALUE - 1) * 10, context),
                    equalTo(1),
                    expectedTermApproximationFooLongTimesTen(LongPoint.newExactQuery("foo", -2))
                    /*
                     * The -2 in the above query comes from the overflow of
                     * (Long.MAX_VALUE - 1) * 10.
                     */
                );

                ft = build("foo_times_negative_two", Map.of(), true);
                assertCountAndApproximation(
                    searcher,
                    ft.termQuery((Long.MAX_VALUE - 1) * -2, context),
                    equalTo(1),
                    expectedTermApproximationFooLongTimesNegativeTwo(LongPoint.newExactQuery("foo", -2))
                );
            }
        }
    }

    private Query should(Query... queries) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (Query q : queries) {
            builder.add(q, Occur.SHOULD);
        }
        return builder.build();
    }

    public void testRangeWithApproximationFromLong() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", 1), new LongPoint("foo", 1)));
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", 2), new LongPoint("foo", 2)));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                SearchExecutionContext context = mockContext(true, new NumberFieldMapper.NumberFieldType("foo", NumberType.LONG));
                MappedFieldType ft = build("foo_plus_five", Map.of(), true);
                assertCountAndApproximation(
                    searcher,
                    ft.rangeQuery(0, 6, false, true, ShapeRelation.CONTAINS, null, null, context),
                    equalTo(1),
                    longRangeQuery("foo", -4, 1)
                );
                assertCountAndApproximation(
                    searcher,
                    ft.rangeQuery(0, 10, false, true, ShapeRelation.CONTAINS, null, null, context),
                    equalTo(2),
                    longRangeQuery("foo", -4, 5)
                );
                assertCountAndApproximation(
                    searcher,
                    ft.rangeQuery(6, 10, false, true, ShapeRelation.CONTAINS, null, null, context),
                    equalTo(1),
                    longRangeQuery("foo", 2, 5)
                );
                assertCountAndApproximation(
                    searcher,
                    ft.rangeQuery(0.1, 11.2, false, true, ShapeRelation.CONTAINS, null, null, context),
                    equalTo(2),
                    longRangeQuery("foo", -4, 6)
                );

                ft = build("foo_times_ten", Map.of(), true);
                assertCountAndApproximation(
                    searcher,
                    ft.rangeQuery(9, 11, false, false, ShapeRelation.CONTAINS, null, null, context),
                    equalTo(1),
                    expectedTermApproximationFooLongTimesTen(longRangeQuery("foo", 1, 1))
                );
                assertCountAndApproximation(
                    searcher,
                    ft.rangeQuery(9, 11, false, true, ShapeRelation.CONTAINS, null, null, context),
                    equalTo(1),
                    expectedTermApproximationFooLongTimesTen(longRangeQuery("foo", 1, 1))
                );
                assertCountAndApproximation(
                    searcher,
                    ft.rangeQuery(0, 100, false, true, ShapeRelation.CONTAINS, null, null, context),
                    equalTo(2),
                    expectedTermApproximationFooLongTimesTen(longRangeQuery("foo", 0, 10))
                );

                ft = build("foo_times_negative_two", Map.of(), true);
                assertCountAndApproximation(
                    searcher,
                    ft.rangeQuery(-3, 0, false, false, ShapeRelation.CONTAINS, null, null, context),
                    equalTo(1),
                    expectedTermApproximationFooLongTimesNegativeTwo(longRangeQuery("foo", 0, 1))
                );
                assertCountAndApproximation(
                    searcher,
                    ft.rangeQuery(-5, -4, false, true, ShapeRelation.CONTAINS, null, null, context),
                    equalTo(1),
                    expectedTermApproximationFooLongTimesNegativeTwo(longRangeQuery("foo", 2, 2))
                );
                assertCountAndApproximation(
                    searcher,
                    ft.rangeQuery(-100, 0, false, true, ShapeRelation.CONTAINS, null, null, context),
                    equalTo(2),
                    expectedTermApproximationFooLongTimesNegativeTwo(longRangeQuery("foo", 0, 49))
                );

                ft = build("foo_divided_by_ten", Map.of(), true);
                assertCountAndApproximation(
                    searcher,
                    ft.rangeQuery(-1, 0, false, true, ShapeRelation.CONTAINS, null, null, context),
                    equalTo(2),
                    longRangeQuery("foo", -9, 9)
                );
                assertCountAndApproximation(
                    searcher,
                    ft.rangeQuery(-1, 1, false, false, ShapeRelation.CONTAINS, null, null, context),
                    equalTo(2),
                    longRangeQuery("foo", -9, 9)
                );
                assertCountAndApproximation(
                    searcher,
                    ft.rangeQuery(0, 2, true, false, ShapeRelation.CONTAINS, null, null, context),
                    equalTo(2),
                    longRangeQuery("foo", -9, 19)
                );
                assertCountAndApproximation(
                    searcher,
                    ft.rangeQuery(0, 1, false, true, ShapeRelation.CONTAINS, null, null, context),
                    equalTo(0),
                    longRangeQuery("foo", 10, 19)
                );

                ft = build("ten_over_foo", Map.of(), true);
                assertCountAndApproximation(
                    searcher,
                    ft.rangeQuery(0, 10, false, true, ShapeRelation.CONTAINS, null, null, context),
                    equalTo(2),
                    new DocValuesFieldExistsQuery("foo")
                );
                assertCountAndApproximation(
                    searcher,
                    ft.rangeQuery(0, 5, false, true, ShapeRelation.CONTAINS, null, null, context),
                    equalTo(1),
                    new DocValuesFieldExistsQuery("foo")
                );
                assertCountAndApproximation(
                    searcher,
                    ft.rangeQuery(0, 1, false, true, ShapeRelation.CONTAINS, null, null, context),
                    equalTo(0),
                    new DocValuesFieldExistsQuery("foo")
                );
            }
        }
    }

    public void testRangeNearMaxIntWithApproximationFromLong() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", Long.MAX_VALUE - 1), new LongPoint("foo", Long.MAX_VALUE - 1)));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                SearchExecutionContext context = mockContext(true, new NumberFieldMapper.NumberFieldType("foo", NumberType.LONG));
                MappedFieldType ft = build("foo_plus_five", Map.of(), true);
                assertCountAndApproximation(
                    searcher,
                    ft.rangeQuery(Long.MAX_VALUE + 4, Long.MAX_VALUE + 10, true, true, ShapeRelation.CONTAINS, null, null, context),
                    equalTo(1),
                    should(
                        longRangeQuery("foo", Long.MIN_VALUE, Long.MIN_VALUE + 4),
                        longRangeQuery("foo", Long.MAX_VALUE - 1, Long.MAX_VALUE)
                    )
                );
            }
        }
    }

    public void testTermWithApproximationFromIntOrSmaller() throws IOException {
        NumberType numberType = randomFrom(NumberType.BYTE, NumberType.SHORT, NumberType.INTEGER);
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", 1), new IntPoint("foo", 1)));
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", 2), new IntPoint("foo", 2)));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                SearchExecutionContext context = mockContext(true, new NumberFieldMapper.NumberFieldType("foo", numberType));
                MappedFieldType ft = build("foo_plus_five", Map.of(), true);
                assertCountAndApproximation(searcher, ft.termQuery(6, context), equalTo(1), IntPoint.newExactQuery("foo", 1));
                assertCountAndQuery(
                    searcher,
                    ft.termQuery(6.1, context),
                    equalTo(0),
                    new MatchNoDocsQuery("Value [6.1] has a decimal part")
                );
                assertCountAndApproximation(searcher, ft.termQuery(7, context), equalTo(1), IntPoint.newExactQuery("foo", 2));
                assertCountAndApproximation(searcher, ft.termQuery(8, context), equalTo(0), IntPoint.newExactQuery("foo", 3));

                ft = build("foo_times_ten", Map.of(), true);
                assertCountAndApproximation(
                    searcher,
                    ft.termQuery(10, context),
                    equalTo(1),
                    expectedTermApproximationFooIntTimesTen(IntPoint.newExactQuery("foo", 1))
                );
                assertCountAndQuery(
                    searcher,
                    ft.termQuery(10.1, context),
                    equalTo(0),
                    new MatchNoDocsQuery("Value [10.1] has a decimal part")
                );
                assertCountAndApproximation(
                    searcher,
                    ft.termQuery(20, context),
                    equalTo(1),
                    expectedTermApproximationFooIntTimesTen(IntPoint.newExactQuery("foo", 2))
                );
                assertCountAndApproximation(
                    searcher,
                    ft.termQuery(30, context),
                    equalTo(0),
                    expectedTermApproximationFooIntTimesTen(IntPoint.newExactQuery("foo", 3))
                );

                ft = build("foo_times_negative_two", Map.of(), true);
                assertCountAndApproximation(
                    searcher,
                    ft.termQuery(-2, context),
                    equalTo(1),
                    expectedTermApproximationFooIntTimesNegativeTwo(IntPoint.newExactQuery("foo", 1))
                );
                assertCountAndApproximation(
                    searcher,
                    ft.termQuery(-4, context),
                    equalTo(1),
                    expectedTermApproximationFooIntTimesNegativeTwo(IntPoint.newExactQuery("foo", 2))
                );

                ft = build("foo_divided_by_ten", Map.of(), true);
                assertCountAndApproximation(searcher, ft.termQuery(0, context), equalTo(2), intRangeQuery("foo", -9, 9));
                assertCountAndApproximation(searcher, ft.termQuery(1, context), equalTo(0), intRangeQuery("foo", 10, 19));
            }
        }
    }

    public void testRangeWithApproximationFromIntOrSmaller() throws IOException {
        NumberType numberType = randomFrom(NumberType.BYTE, NumberType.SHORT, NumberType.INTEGER);
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", 1), new IntPoint("foo", 1)));
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", 2), new IntPoint("foo", 2)));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                SearchExecutionContext context = mockContext(true, new NumberFieldMapper.NumberFieldType("foo", numberType));
                MappedFieldType ft = build("foo_plus_five", Map.of(), true);
                assertCountAndApproximation(
                    searcher,
                    ft.rangeQuery(0, 6, false, true, ShapeRelation.CONTAINS, null, null, context),
                    equalTo(1),
                    intRangeQuery("foo", -4, 1)
                );
                assertCountAndApproximation(
                    searcher,
                    ft.rangeQuery(0, 10, false, true, ShapeRelation.CONTAINS, null, null, context),
                    equalTo(2),
                    intRangeQuery("foo", -4, 5)
                );
                assertCountAndApproximation(
                    searcher,
                    ft.rangeQuery(6, 10, false, true, ShapeRelation.CONTAINS, null, null, context),
                    equalTo(1),
                    intRangeQuery("foo", 2, 5)
                );
                assertCountAndApproximation(
                    searcher,
                    ft.rangeQuery(0.1, 11.2, false, true, ShapeRelation.CONTAINS, null, null, context),
                    equalTo(2),
                    intRangeQuery("foo", -4, 6)
                );

                ft = build("foo_times_ten", Map.of(), true);
                assertCountAndApproximation(
                    searcher,
                    ft.rangeQuery(9, 11, false, false, ShapeRelation.CONTAINS, null, null, context),
                    equalTo(1),
                    expectedTermApproximationFooIntTimesTen(intRangeQuery("foo", 1, 1))
                );
                assertCountAndApproximation(
                    searcher,
                    ft.rangeQuery(9, 11, false, true, ShapeRelation.CONTAINS, null, null, context),
                    equalTo(1),
                    expectedTermApproximationFooIntTimesTen(intRangeQuery("foo", 1, 1))
                );
                assertCountAndApproximation(
                    searcher,
                    ft.rangeQuery(0, 100, false, true, ShapeRelation.CONTAINS, null, null, context),
                    equalTo(2),
                    expectedTermApproximationFooIntTimesTen(intRangeQuery("foo", 0, 10))
                );

                ft = build("foo_times_negative_two", Map.of(), true);
                assertCountAndApproximation(
                    searcher,
                    ft.rangeQuery(-3, 0, false, false, ShapeRelation.CONTAINS, null, null, context),
                    equalTo(1),
                    expectedTermApproximationFooIntTimesNegativeTwo(intRangeQuery("foo", 0, 1))
                );
                assertCountAndApproximation(
                    searcher,
                    ft.rangeQuery(-5, -4, false, true, ShapeRelation.CONTAINS, null, null, context),
                    equalTo(1),
                    expectedTermApproximationFooIntTimesNegativeTwo(intRangeQuery("foo", 2, 2))
                );
                assertCountAndApproximation(
                    searcher,
                    ft.rangeQuery(-100, 0, false, true, ShapeRelation.CONTAINS, null, null, context),
                    equalTo(2),
                    expectedTermApproximationFooIntTimesNegativeTwo(intRangeQuery("foo", 0, 49))
                );

                ft = build("foo_divided_by_ten", Map.of(), true);
                assertCountAndApproximation(
                    searcher,
                    ft.rangeQuery(-1, 0, false, true, ShapeRelation.CONTAINS, null, null, context),
                    equalTo(2),
                    intRangeQuery("foo", -9, 9)
                );
                assertCountAndApproximation(
                    searcher,
                    ft.rangeQuery(-1, 1, false, false, ShapeRelation.CONTAINS, null, null, context),
                    equalTo(2),
                    intRangeQuery("foo", -9, 9)
                );
                assertCountAndApproximation(
                    searcher,
                    ft.rangeQuery(0, 2, true, false, ShapeRelation.CONTAINS, null, null, context),
                    equalTo(2),
                    intRangeQuery("foo", -9, 19)
                );
                assertCountAndApproximation(
                    searcher,
                    ft.rangeQuery(0, 1, false, true, ShapeRelation.CONTAINS, null, null, context),
                    equalTo(0),
                    intRangeQuery("foo", 10, 19)
                );
            }
        }
    }

    private Query expectedTermApproximationFooIntTimesTen(Query precise) {
        return should(precise, new BoostQuery(new MatchNoDocsQuery(), 2f));
    }

    private Query expectedTermApproximationFooIntTimesNegativeTwo(Query precise) {
        return should(precise, new BoostQuery(new MatchNoDocsQuery(), 2f));
    }

    private Query longRangeQuery(String field, long lower, long upper) {
        return new IndexOrDocValuesQuery(
            LongPoint.newRangeQuery(field, lower, upper),
            SortedNumericDocValuesField.newSlowRangeQuery(field, lower, upper)
        );
    }

    private Query intRangeQuery(String field, int lower, int upper) {
        return new IndexOrDocValuesQuery(
            IntPoint.newRangeQuery(field, lower, upper),
            SortedNumericDocValuesField.newSlowRangeQuery(field, lower, upper)
        );
    }

    private void assertCountAndQuery(IndexSearcher searcher, Query query, Matcher<Integer> count, Query expectedQuery) throws IOException {
        assertThat(searcher.count(query), count);

        assertThat(query, equalTo(expectedQuery));
    }

    public static void assertCountAndApproximation(IndexSearcher searcher, Query query, Matcher<Integer> count, Query expectedApproximation)
        throws IOException {
        assertThat(searcher.count(query), count);
        assertThat(query, instanceOf(AbstractScriptFieldQuery.class));

        expectedApproximation = searcher.rewrite(expectedApproximation);
        assertThat(((AbstractScriptFieldQuery<?>) searcher.rewrite(query)).approximation(), equalTo(expectedApproximation));
    }

    @Override
    protected Query randomTermQuery(MappedFieldType ft, SearchExecutionContext ctx) {
        return ft.termQuery(randomLong(), ctx);
    }

    @Override
    public void testTermsQuery() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [1]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [2]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                assertThat(searcher.count(simpleMappedFieldType().termsQuery(List.of("1"), mockContext())), equalTo(1));
                assertThat(searcher.count(simpleMappedFieldType().termsQuery(List.of(1), mockContext())), equalTo(1));
                assertThat(searcher.count(simpleMappedFieldType().termsQuery(List.of(1.1), mockContext())), equalTo(0));
                assertThat(searcher.count(simpleMappedFieldType().termsQuery(List.of(1.1, 2), mockContext())), equalTo(1));
                assertThat(searcher.count(simpleMappedFieldType().termsQuery(List.of(2, 1), mockContext())), equalTo(2));
            }
        }
    }

    @Override
    protected Query randomTermsQuery(MappedFieldType ft, SearchExecutionContext ctx) {
        return ft.termsQuery(List.of(randomLong()), ctx);
    }

    @Override
    protected LongScriptFieldType simpleMappedFieldType() {
        return build("read_foo", Map.of(), randomBoolean());
    }

    @Override
    protected LongScriptFieldType loopFieldType() {
        return build("loop", Map.of(), randomBoolean());
    }

    @Override
    protected String typeName() {
        return "long";
    }

    private static LongScriptFieldType build(String code, Map<String, Object> params, boolean approximateFirst) {
        return build(new Script(ScriptType.INLINE, "test", code, params), approximateFirst);
    }

    private static LongScriptFieldType build(Script script, boolean approximateFirst) {
        return new LongScriptFieldType("test", factory(script), script, approximateFirst, emptyMap());
    }

    private static LongFieldScript.Factory factory(Script script) {
        switch (script.getIdOrCode()) {
            case "read_foo":
                return (fieldName, params, lookup) -> (ctx) -> new LongFieldScript(fieldName, params, lookup, ctx) {
                    @Override
                    public void execute() {
                        for (Object foo : (List<?>) lookup.source().get("foo")) {
                            emit(((Number) foo).longValue());
                        }
                    }
                };
            case "foo_plus_five":
                return new LongFieldScript.Factory() {
                    @Override
                    public LeafFactory newFactory(String fieldName, Map<String, Object> params, SearchLookup lookup) {
                        return (ctx) -> new LongFieldScript(fieldName, params, lookup, ctx) {
                            @Override
                            public void execute() {
                                for (long foo : (ScriptDocValues.Longs) getDoc().get("foo")) {
                                    emit(foo + 5);
                                }
                            }
                        };
                    }

                    @Override
                    public QueryableExpressionBuilder emitExpression() {
                        return QueryableExpressionBuilder.add(
                            QueryableExpressionBuilder.field("foo"),
                            QueryableExpressionBuilder.constant(5L)
                        );
                    }
                };
            case "foo_times_ten":
                return new LongFieldScript.Factory() {
                    @Override
                    public LeafFactory newFactory(String fieldName, Map<String, Object> params, SearchLookup lookup) {
                        return (ctx) -> new LongFieldScript(fieldName, params, lookup, ctx) {
                            @Override
                            public void execute() {
                                for (long foo : (ScriptDocValues.Longs) getDoc().get("foo")) {
                                    emit(foo * 10);
                                }
                            }
                        };
                    }

                    @Override
                    public QueryableExpressionBuilder emitExpression() {
                        return QueryableExpressionBuilder.multiply(
                            QueryableExpressionBuilder.field("foo"),
                            QueryableExpressionBuilder.constant(10L)
                        );
                    }
                };
            case "foo_times_negative_two":
                return new LongFieldScript.Factory() {
                    @Override
                    public LeafFactory newFactory(String fieldName, Map<String, Object> params, SearchLookup lookup) {
                        return (ctx) -> new LongFieldScript(fieldName, params, lookup, ctx) {
                            @Override
                            public void execute() {
                                for (long foo : (ScriptDocValues.Longs) getDoc().get("foo")) {
                                    emit(foo * -2);
                                }
                            }
                        };
                    }

                    @Override
                    public QueryableExpressionBuilder emitExpression() {
                        return QueryableExpressionBuilder.multiply(
                            QueryableExpressionBuilder.field("foo"),
                            QueryableExpressionBuilder.constant(-2L)
                        );
                    }
                };
            case "foo_divided_by_ten":
                return new LongFieldScript.Factory() {
                    @Override
                    public LeafFactory newFactory(String fieldName, Map<String, Object> params, SearchLookup lookup) {
                        return (ctx) -> new LongFieldScript(fieldName, params, lookup, ctx) {
                            @Override
                            public void execute() {
                                for (long foo : (ScriptDocValues.Longs) getDoc().get("foo")) {
                                    emit(foo / 10);
                                }
                            }
                        };
                    }

                    @Override
                    public QueryableExpressionBuilder emitExpression() {
                        return QueryableExpressionBuilder.divide(
                            QueryableExpressionBuilder.field("foo"),
                            QueryableExpressionBuilder.constant(10L)
                        );
                    }
                };
            case "ten_over_foo":
                return new LongFieldScript.Factory() {
                    @Override
                    public LeafFactory newFactory(String fieldName, Map<String, Object> params, SearchLookup lookup) {
                        return (ctx) -> new LongFieldScript(fieldName, params, lookup, ctx) {
                            @Override
                            public void execute() {
                                for (long foo : (ScriptDocValues.Longs) getDoc().get("foo")) {
                                    emit(10 / foo);
                                }
                            }
                        };
                    }

                    @Override
                    public QueryableExpressionBuilder emitExpression() {
                        return QueryableExpressionBuilder.unknownOp(QueryableExpressionBuilder.field("foo"));
                    }
                };
            case "add_param":
                return (fieldName, params, lookup) -> (ctx) -> new LongFieldScript(fieldName, params, lookup, ctx) {
                    @Override
                    public void execute() {
                        for (Object foo : (List<?>) lookup.source().get("foo")) {
                            emit(((Number) foo).longValue() + ((Number) getParams().get("param")).longValue());
                        }
                    }
                };
            case "millis_ago":
                // Painless actually call System.currentTimeMillis. We could mock the time but this works fine too.
                long now = System.currentTimeMillis();
                return (fieldName, params, lookup) -> (ctx) -> new LongFieldScript(fieldName, params, lookup, ctx) {
                    @Override
                    public void execute() {
                        for (Object timestamp : (List<?>) lookup.source().get("timestamp")) {
                            emit(now - ((Number) timestamp).longValue());
                        }
                    }
                };
            case "loop":
                return (fieldName, params, lookup) -> {
                    // Indicate that this script wants the field call "test", which *is* the name of this field
                    lookup.forkAndTrackFieldReferences("test");
                    throw new IllegalStateException("shoud have thrown on the line above");
                };
            default:
                throw new IllegalArgumentException("unsupported script [" + script.getIdOrCode() + "]");
        }
    }
}
