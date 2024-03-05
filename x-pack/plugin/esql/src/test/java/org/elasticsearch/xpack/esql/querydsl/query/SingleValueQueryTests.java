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
import org.elasticsearch.xpack.ql.querydsl.query.MatchAll;
import org.elasticsearch.xpack.ql.querydsl.query.RangeQuery;
import org.elasticsearch.xpack.ql.tree.Source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;

public class SingleValueQueryTests extends MapperServiceTestCase {
    interface Setup {
        XContentBuilder mapping(XContentBuilder builder) throws IOException;

        List<List<Object>> build(RandomIndexWriter iw) throws IOException;

        void assertStats(SingleValueQuery.Builder builder, boolean subHasTwoPhase);
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
        testCase(new SingleValueQuery(new MatchAll(Source.EMPTY), "foo").asBuilder(), false, false, this::runCase);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/105952")
    public void testMatchSome() throws IOException {
        int max = between(1, 100);
        testCase(
            new SingleValueQuery.Builder(new RangeQueryBuilder("i").lt(max), "foo", new SingleValueQuery.Stats(), Source.EMPTY),
            false,
            false,
            (fieldValues, count) -> runCase(fieldValues, count, null, max, false)
        );
    }

    public void testSubPhrase() throws IOException {
        testCase(
            new SingleValueQuery.Builder(
                new MatchPhraseQueryBuilder("str", "fox jumped"),
                "foo",
                new SingleValueQuery.Stats(),
                Source.EMPTY
            ),
            false,
            true,
            this::runCase
        );
    }

    public void testMatchNone() throws IOException {
        testCase(
            new SingleValueQuery.Builder(new MatchNoneQueryBuilder(), "foo", new SingleValueQuery.Stats(), Source.EMPTY),
            true,
            false,
            (fieldValues, count) -> assertThat(count, equalTo(0))
        );
    }

    public void testRewritesToMatchNone() throws IOException {
        testCase(
            new SingleValueQuery.Builder(new TermQueryBuilder("missing", 0), "foo", new SingleValueQuery.Stats(), Source.EMPTY),
            true,
            false,
            (fieldValues, count) -> assertThat(count, equalTo(0))
        );
    }

    public void testNotMatchAll() throws IOException {
        testCase(
            new SingleValueQuery(new MatchAll(Source.EMPTY), "foo").negate(Source.EMPTY).asBuilder(),
            true,
            false,
            (fieldValues, count) -> assertThat(count, equalTo(0))
        );
    }

    public void testNotMatchNone() throws IOException {
        testCase(
            new SingleValueQuery(new MatchAll(Source.EMPTY).negate(Source.EMPTY), "foo").negate(Source.EMPTY).asBuilder(),
            false,
            false,
            this::runCase
        );
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/105918")
    public void testNotMatchSome() throws IOException {
        int max = between(1, 100);
        testCase(
            new SingleValueQuery(new RangeQuery(Source.EMPTY, "i", null, false, max, false, null), "foo").negate(Source.EMPTY).asBuilder(),
            false,
            true,
            (fieldValues, count) -> runCase(fieldValues, count, max, 100, true)
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
     * @param scanForMVs Should the check for Warnings scan the entire fieldValues? This will override the docsStart:docsStop interval,
     *                   which is needed for some cases.
     */
    private void runCase(List<List<Object>> fieldValues, int count, Integer docsStart, Integer docsStop, boolean scanForMVs) {
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

        // the SingleValueQuery.TwoPhaseIteratorForSortedNumericsAndTwoPhaseQueries can scan all docs - and generate warnings - even if
        // inner query matches none, so warn if MVs have been encountered within given range, OR if a full scan is required
        if (mvCountInRange > 0 || (scanForMVs && fieldValues.stream().anyMatch(x -> x.size() > 1))) {
            assertWarnings(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: java.lang.IllegalArgumentException: single-value function encountered multi-value"
            );
        }
    }

    private void runCase(List<List<Object>> fieldValues, int count) {
        runCase(fieldValues, count, null, null, false);
    }

    private void testCase(SingleValueQuery.Builder builder, boolean rewritesToMatchNone, boolean subHasTwoPhase, TestCase testCase)
        throws IOException {
        MapperService mapper = createMapperService(mapping(setup::mapping));
        try (Directory d = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), d)) {
            List<List<Object>> fieldValues = setup.build(iw);
            try (IndexReader reader = iw.getReader()) {
                SearchExecutionContext ctx = createSearchExecutionContext(mapper, new IndexSearcher(reader));
                QueryBuilder rewritten = builder.rewrite(ctx);
                Query query = rewritten.toQuery(ctx);
                testCase.run(fieldValues, ctx.searcher().count(query));
                if (rewritesToMatchNone) {
                    assertThat(rewritten, instanceOf(MatchNoneQueryBuilder.class));
                    assertThat(builder.stats().missingField(), equalTo(0));
                    assertThat(builder.stats().rewrittenToMatchNone(), equalTo(1));
                    assertThat(builder.stats().numericSingle(), equalTo(0));
                    assertThat(builder.stats().numericMultiNoApprox(), equalTo(0));
                    assertThat(builder.stats().numericMultiApprox(), equalTo(0));
                    assertThat(builder.stats().ordinalsSingle(), equalTo(0));
                    assertThat(builder.stats().ordinalsMultiNoApprox(), equalTo(0));
                    assertThat(builder.stats().ordinalsMultiApprox(), equalTo(0));
                    assertThat(builder.stats().bytesApprox(), equalTo(0));
                    assertThat(builder.stats().bytesNoApprox(), equalTo(0));
                } else {
                    assertThat(builder.stats().rewrittenToMatchNone(), equalTo(0));
                    setup.assertStats(builder, subHasTwoPhase);
                }
                assertThat(builder.stats().noNextScorer(), equalTo(0));
            }
        }
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

        @Override
        public void assertStats(SingleValueQuery.Builder builder, boolean subHasTwoPhase) {
            assertThat(builder.stats().missingField(), equalTo(0));
            switch (fieldType) {
                case "long", "integer", "short", "byte", "double", "float" -> {
                    assertThat(builder.stats().ordinalsSingle(), equalTo(0));
                    assertThat(builder.stats().ordinalsMultiNoApprox(), equalTo(0));
                    assertThat(builder.stats().ordinalsMultiApprox(), equalTo(0));
                    assertThat(builder.stats().bytesApprox(), equalTo(0));
                    assertThat(builder.stats().bytesNoApprox(), equalTo(0));

                    if (multivaluedField || empty) {
                        assertThat(builder.stats().numericSingle(), greaterThanOrEqualTo(0));
                        if (subHasTwoPhase) {
                            assertThat(builder.stats().numericMultiNoApprox(), equalTo(0));
                            assertThat(builder.stats().numericMultiApprox(), greaterThan(0));
                        } else {
                            assertThat(builder.stats().numericMultiNoApprox(), greaterThan(0));
                            assertThat(builder.stats().numericMultiApprox(), equalTo(0));
                        }
                    } else {
                        assertThat(builder.stats().numericSingle(), greaterThan(0));
                        assertThat(builder.stats().numericMultiNoApprox(), equalTo(0));
                        assertThat(builder.stats().numericMultiApprox(), equalTo(0));
                    }
                }
                case "keyword" -> {
                    assertThat(builder.stats().numericSingle(), equalTo(0));
                    assertThat(builder.stats().numericMultiNoApprox(), equalTo(0));
                    assertThat(builder.stats().numericMultiApprox(), equalTo(0));
                    assertThat(builder.stats().bytesApprox(), equalTo(0));
                    assertThat(builder.stats().bytesNoApprox(), equalTo(0));
                    if (multivaluedField || empty) {
                        assertThat(builder.stats().ordinalsSingle(), greaterThanOrEqualTo(0));
                        if (subHasTwoPhase) {
                            assertThat(builder.stats().ordinalsMultiNoApprox(), equalTo(0));
                            assertThat(builder.stats().ordinalsMultiApprox(), greaterThan(0));
                        } else {
                            assertThat(builder.stats().ordinalsMultiNoApprox(), greaterThan(0));
                            assertThat(builder.stats().ordinalsMultiApprox(), equalTo(0));
                        }
                    } else {
                        assertThat(builder.stats().ordinalsSingle(), greaterThan(0));
                        assertThat(builder.stats().ordinalsMultiNoApprox(), equalTo(0));
                        assertThat(builder.stats().ordinalsMultiApprox(), equalTo(0));
                    }
                }
                default -> throw new UnsupportedOperationException();
            }
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

        @Override
        public void assertStats(SingleValueQuery.Builder builder, boolean subHasTwoPhase) {
            assertThat(builder.stats().missingField(), equalTo(1));
            assertThat(builder.stats().numericSingle(), equalTo(0));
            assertThat(builder.stats().numericMultiNoApprox(), equalTo(0));
            assertThat(builder.stats().numericMultiApprox(), equalTo(0));
            assertThat(builder.stats().ordinalsSingle(), equalTo(0));
            assertThat(builder.stats().ordinalsMultiNoApprox(), equalTo(0));
            assertThat(builder.stats().ordinalsMultiApprox(), equalTo(0));
            assertThat(builder.stats().bytesApprox(), equalTo(0));
            assertThat(builder.stats().bytesNoApprox(), equalTo(0));
        }
    }
}
