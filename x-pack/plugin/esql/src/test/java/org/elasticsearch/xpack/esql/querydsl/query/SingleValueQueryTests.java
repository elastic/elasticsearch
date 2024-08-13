/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.querydsl.query;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.core.querydsl.query.MatchAll;
import org.elasticsearch.xpack.esql.core.querydsl.query.RangeQuery;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class SingleValueQueryTests extends MapperServiceTestCase {
    interface Setup {
        XContentBuilder mapping(XContentBuilder builder) throws IOException;

        List<List<Object>> build(RandomIndexWriter iw) throws IOException;
    }

    @ParametersFactory
    public static List<Object[]> params() {
        List<Object[]> params = new ArrayList<>();
        for (String fieldType : new String[] { "long", "integer", "short", "byte", "double", "float", "keyword" }) {
            for (boolean multivaluedField : new boolean[] { true, false }) {
                for (boolean allowEmpty : new boolean[] { true, false }) {
                    params.add(new Object[] { new StandardSetup(fieldType, multivaluedField, allowEmpty, 100) });
                }
            }
        }
        params.add(new Object[] { new FieldMissingSetup() });
        return params;
    }

    private final Setup setup;

    public SingleValueQueryTests(Setup setup) {
        this.setup = setup;
    }

    public void testMatchAll() throws IOException {
        testCase(new SingleValueQuery(new MatchAll(Source.EMPTY), "foo").asBuilder(), this::runCase);
    }

    public void testMatchSome() throws IOException {
        int max = between(1, 100);
        testCase(
            new SingleValueQuery.Builder(new RangeQueryBuilder("i").lt(max), "foo", Source.EMPTY),
            (fieldValues, count) -> runCase(fieldValues, count, null, max)
        );
    }

    public void testSubPhrase() throws IOException {
        testCase(new SingleValueQuery.Builder(new MatchPhraseQueryBuilder("str", "fox jumped"), "foo", Source.EMPTY), this::runCase);
    }

    public void testMatchNone() throws IOException {
        testCase(
            new SingleValueQuery.Builder(new MatchNoneQueryBuilder(), "foo", Source.EMPTY),
            (fieldValues, count) -> assertThat(count, equalTo(0))
        );
    }

    public void testRewritesToMatchNone() throws IOException {
        testCase(
            new SingleValueQuery.Builder(new TermQueryBuilder("missing", 0), "foo", Source.EMPTY),
            (fieldValues, count) -> assertThat(count, equalTo(0))
        );
    }

    public void testNotMatchAll() throws IOException {
        testCase(
            new SingleValueQuery(new MatchAll(Source.EMPTY), "foo").negate(Source.EMPTY).asBuilder(),
            (fieldValues, count) -> assertThat(count, equalTo(0))
        );
    }

    public void testNotMatchNone() throws IOException {
        testCase(
            new SingleValueQuery(new MatchAll(Source.EMPTY).negate(Source.EMPTY), "foo").negate(Source.EMPTY).asBuilder(),
            this::runCase
        );
    }

    public void testNotMatchSome() throws IOException {
        int max = between(1, 100);
        testCase(
            new SingleValueQuery(new RangeQuery(Source.EMPTY, "i", null, false, max, false, null), "foo").negate(Source.EMPTY).asBuilder(),
            (fieldValues, count) -> runCase(fieldValues, count, max, 100)
        );
    }

    @FunctionalInterface
    interface TestCase {
        void run(List<List<Object>> fieldValues, int count) throws IOException;
    }

    /**
     * Helper to run the checks of some of the test cases. This will perform two verifications: one about the count of the values the query
     * is supposed to match and one on the Warnings that are supposed to be raised.
     * @param fieldValues The indexed values of the field the query runs against.
     * @param count The count of the docs the query matched.
     * @param docsStart The start of the slice in fieldValues we want to consider. If `null`, the start will be 0.
     * @param docsStop The end of the slice in fieldValues we want to consider. If `null`, the end will be the fieldValues size.
     */
    private void runCase(List<List<Object>> fieldValues, int count, Integer docsStart, Integer docsStop) {
        int expected = 0;
        int min = docsStart != null ? docsStart : 0;
        int max = docsStop != null ? docsStop : fieldValues.size();
        int mvCountInRange = 0;
        for (int i = min; i < max; i++) {
            int valuesCount = fieldValues.get(i).size();
            if (valuesCount == 1) {
                expected++;
            } else if (valuesCount > 1) {
                mvCountInRange++;
            }
        }
        assertThat(count, equalTo(expected));

        // we should only have warnings if we have matched a multi-value
        if (mvCountInRange > 0) {
            assertWarnings(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: java.lang.IllegalArgumentException: single-value function encountered multi-value"
            );
        }
    }

    private void runCase(List<List<Object>> fieldValues, int count) {
        runCase(fieldValues, count, null, null);
    }

    private void testCase(SingleValueQuery.Builder builder, TestCase testCase) throws IOException {
        MapperService mapper = createMapperService(mapping(setup::mapping));
        try (Directory d = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), d)) {
            List<List<Object>> fieldValues = setup.build(iw);
            try (IndexReader reader = iw.getReader()) {
                SearchExecutionContext ctx = createSearchExecutionContext(mapper, new IndexSearcher(reader));
                QueryBuilder rewritten = builder.rewrite(ctx);
                Query query = rewritten.toQuery(ctx);
                testCase.run(fieldValues, ctx.searcher().count(query));
                assertEqualsAndHashcodeStable(query, rewritten.toQuery(ctx));
            }
        }
    }

    private void assertEqualsAndHashcodeStable(Query query1, Query query2) {
        assertEquals(query1, query2);
        assertEquals(query1.hashCode(), query2.hashCode());
    }

    private record StandardSetup(String fieldType, boolean multivaluedField, boolean empty, int count) implements Setup {
        @Override
        public XContentBuilder mapping(XContentBuilder builder) throws IOException {
            builder.startObject("i").field("type", "long").endObject();
            builder.startObject("str").field("type", "text").endObject();
            return builder.startObject("foo").field("type", fieldType).endObject();
        }

        @Override
        public List<List<Object>> build(RandomIndexWriter iw) throws IOException {
            List<List<Object>> fieldValues = new ArrayList<>(100);
            for (int i = 0; i < count; i++) {
                List<Object> values = values(i);
                fieldValues.add(values);
                iw.addDocument(docFor(i, values));
            }
            return fieldValues;
        }

        private List<Object> values(int i) {
            // i == 10 forces at least one multivalued field when we're configured for multivalued fields
            boolean makeMultivalued = multivaluedField && (i == 10 || randomBoolean());
            if (makeMultivalued) {
                int count = between(2, 10);
                Set<Object> set = new HashSet<>(count);
                while (set.size() < count) {
                    set.add(randomValue());
                }
                return List.copyOf(set);
            }
            // i == 0 forces at least one empty field when we're configured for empty fields
            if (empty && (i == 0 || randomBoolean())) {
                return List.of();
            }
            return List.of(randomValue());
        }

        private Object randomValue() {
            return switch (fieldType) {
                case "long" -> randomLong();
                case "integer" -> randomInt();
                case "short" -> randomShort();
                case "byte" -> randomByte();
                case "double" -> randomDouble();
                case "float" -> randomFloat();
                case "keyword" -> randomAlphaOfLength(5);
                default -> throw new UnsupportedOperationException();
            };
        }

        private List<IndexableField> docFor(int i, Iterable<Object> values) {
            List<IndexableField> fields = new ArrayList<>();
            fields.add(new LongField("i", i));
            fields.add(new TextField("str", "the quick brown fox jumped over the lazy dog", Field.Store.NO));
            switch (fieldType) {
                case "long", "integer", "short", "byte" -> {
                    for (Object v : values) {
                        long l = ((Number) v).longValue();
                        fields.add(new LongField("foo", l, Field.Store.NO));
                    }
                }
                case "double", "float" -> {
                    for (Object v : values) {
                        double d = ((Number) v).doubleValue();
                        fields.add(new DoubleField("foo", d, Field.Store.NO));
                    }
                }
                case "keyword" -> {
                    for (Object v : values) {
                        fields.add(new KeywordField("foo", v.toString(), Field.Store.NO));
                    }
                }
                default -> throw new UnsupportedOperationException();
            }
            return fields;
        }
    }

    private record FieldMissingSetup() implements Setup {
        @Override
        public XContentBuilder mapping(XContentBuilder builder) throws IOException {
            builder.startObject("str").field("type", "text").endObject();
            return builder.startObject("i").field("type", "long").endObject();
        }

        @Override
        public List<List<Object>> build(RandomIndexWriter iw) throws IOException {
            List<List<Object>> fieldValues = new ArrayList<>(100);
            for (int i = 0; i < 100; i++) {
                iw.addDocument(
                    List.of(new LongField("i", i), new TextField("str", "the quick brown fox jumped over the lazy dog", Field.Store.NO))
                );
                fieldValues.add(List.of());
            }
            return fieldValues;
        }
    }
}
