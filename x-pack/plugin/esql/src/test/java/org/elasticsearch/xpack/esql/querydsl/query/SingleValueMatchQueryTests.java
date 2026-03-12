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
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.WarningSourceLocation;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.compute.querydsl.query.SingleValueMatchQuery;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.index.mapper.FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF;
import static org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

public class SingleValueMatchQueryTests extends MapperServiceTestCase {
    interface Setup {
        XContentBuilder mapping(XContentBuilder builder) throws IOException;

        List<List<Object>> build(RandomIndexWriter iw) throws IOException;

        void assertRewrite(IndexSearcher indexSearcher, Query query) throws IOException;
    }

    @ParametersFactory(argumentFormatting = "%s")
    public static List<Object[]> params() {
        List<Object[]> params = new ArrayList<>();
        for (String fieldType : new String[] { "long", "integer", "short", "byte", "double", "float", "keyword" }) {
            params.add(new Object[] { new SneakyTwo(fieldType) });
            for (boolean multivaluedField : new boolean[] { true, false }) {
                for (boolean allowEmpty : new boolean[] { true, false }) {
                    for (DocValuesMode docValuesMode : new DocValuesMode[] { DocValuesMode.DEFAULT, DocValuesMode.DOC_VALUES_ONLY }) {
                        params.add(new Object[] { new StandardSetup(fieldType, multivaluedField, docValuesMode, allowEmpty, 100) });
                    }
                    if (fieldType.equals("keyword") && EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled()) {
                        params.add(
                            new Object[] {
                                new StandardSetup(
                                    fieldType,
                                    multivaluedField,
                                    DocValuesMode.DOC_VALUES_ONLY_HIGH_CARDINALITY,
                                    allowEmpty,
                                    100
                                ) }
                        );
                    }
                }
            }
        }
        return params;
    }

    private final Setup setup;

    public SingleValueMatchQueryTests(Setup setup) {
        this.setup = setup;
    }

    public void testQuery() throws IOException {
        MapperService mapper = createMapperService(mapping(setup::mapping));
        try (Directory d = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), d)) {
            List<List<Object>> fieldValues = setup.build(iw);
            try (IndexReader reader = iw.getReader()) {
                SearchExecutionContext ctx = createSearchExecutionContext(mapper, new IndexSearcher(reader));
                Query query = new SingleValueMatchQuery(
                    ctx.getForField(mapper.fieldType("foo"), MappedFieldType.FielddataOperation.SEARCH),
                    Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new TestWarningsSource("test")),
                    "single-value function encountered multi-value"
                );
                runCase(fieldValues, ctx.searcher().count(query));
                setup.assertRewrite(ctx.searcher(), query);
            }
        }
    }

    public void testEmpty() throws IOException {
        MapperService mapper = createMapperService(mapping(setup::mapping));
        try (Directory d = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), d)) {
            try (IndexReader reader = iw.getReader()) {
                SearchExecutionContext ctx = createSearchExecutionContext(mapper, new IndexSearcher(reader));
                Query query = new SingleValueMatchQuery(
                    ctx.getForField(mapper.fieldType("foo"), MappedFieldType.FielddataOperation.SEARCH),
                    Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new TestWarningsSource("test")),
                    "single-value function encountered multi-value"
                );
                runCase(List.of(), ctx.searcher().count(query));
            }
        }
    }

    private record TestWarningsSource(String text, String viewName, int lineNumber, int columnNumber) implements WarningSourceLocation {
        private TestWarningsSource(String text) {
            this(text, null, 1, 1);
        }
    }

    private void runCase(List<List<Object>> fieldValues, int count) {
        int expected = 0;
        int mvCountInRange = 0;
        for (int i = 0; i < fieldValues.size(); i++) {
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
        if (mvCountInRange > 0) {
            assertWarnings(
                "Line 1:1: evaluation of [test] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: java.lang.IllegalArgumentException: single-value function encountered multi-value"
            );
        }
    }

    private record StandardSetup(String fieldType, boolean multivaluedField, DocValuesMode docValuesMode, boolean empty, int count)
        implements
            Setup {
        @Override
        public XContentBuilder mapping(XContentBuilder builder) throws IOException {
            return switch (docValuesMode) {
                case DOC_VALUES_ONLY_HIGH_CARDINALITY -> builder.startObject("foo")
                    .field("type", fieldType)
                    .startObject("doc_values")
                    .field("cardinality", "high")
                    .endObject()
                    .endObject();
                case DOC_VALUES_ONLY -> builder.startObject("foo").field("type", fieldType).field("doc_values", true).endObject();
                case DEFAULT -> builder.startObject("foo").field("type", fieldType).endObject();
            };
        }

        @Override
        public List<List<Object>> build(RandomIndexWriter iw) throws IOException {
            List<List<Object>> docs = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                List<Object> values = values(i);
                docs.add(values);
                iw.addDocument(docFor(values, docValuesMode));
            }
            return docs;
        }

        @Override
        public void assertRewrite(IndexSearcher indexSearcher, Query query) throws IOException {
            if (empty == false && multivaluedField == false) {
                assertThat(query.rewrite(indexSearcher), instanceOf(MatchAllDocsQuery.class));
            } else {
                assertThat(query.rewrite(indexSearcher), sameInstance(query));
            }
        }

        private List<Object> values(int i) {
            // i == 10 forces at least one multivalued field when we're configured for multivalued fields
            boolean makeMultivalued = multivaluedField && (i == 10 || randomBoolean());
            if (makeMultivalued) {
                int count = between(2, 10);
                Set<Object> set = new HashSet<>(count);
                while (set.size() < count) {
                    set.add(randomValue(fieldType));
                }
                return List.copyOf(set);
            }
            // i == 0 forces at least one empty field when we're configured for empty fields
            if (empty && (i == 0 || randomBoolean())) {
                return List.of();
            }
            return List.of(randomValue(fieldType));
        }
    }

    enum DocValuesMode {
        DEFAULT,
        DOC_VALUES_ONLY,
        DOC_VALUES_ONLY_HIGH_CARDINALITY,
    }

    /**
     * Tests a scenario where we were incorrectly rewriting {@code keyword} fields to
     * {@link MatchAllDocsQuery} when:
     * <ul>
     *     <li>Is defined on every field</li>
     *     <li>Contains the same number of distinct values as documents</li>
     * </ul>
     */
    private record SneakyTwo(String fieldType) implements Setup {
        @Override
        public XContentBuilder mapping(XContentBuilder builder) throws IOException {
            return builder.startObject("foo").field("type", fieldType).endObject();
        }

        @Override
        public List<List<Object>> build(RandomIndexWriter iw) throws IOException {
            Object first = randomValue(fieldType);
            Object second = randomValue(fieldType);
            List<Object> justFirst = List.of(first);
            List<Object> both = List.of(first, second);
            iw.addDocument(docFor(justFirst, DocValuesMode.DEFAULT));
            iw.addDocument(docFor(both, DocValuesMode.DEFAULT));
            return List.of(justFirst, both);
        }

        @Override
        public void assertRewrite(IndexSearcher indexSearcher, Query query) throws IOException {
            // There are multivalued fields
            assertThat(query.rewrite(indexSearcher), sameInstance(query));
        }
    }

    private static Object randomValue(String fieldType) {
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

    private static List<IndexableField> docFor(Iterable<Object> values, DocValuesMode docValuesMode) {
        long count = 0;
        var mvField = new MultiValuedBinaryDocValuesField.SeparateCount("foo", false);
        List<IndexableField> fields = new ArrayList<>();

        for (Object v : values) {
            switch (docValuesMode) {
                case DOC_VALUES_ONLY_HIGH_CARDINALITY -> {
                    switch (v) {
                        case String s -> {
                            mvField.add(new BytesRef(s));
                            count++;
                        }
                        default -> throw new UnsupportedOperationException();
                    }
                }
                case DOC_VALUES_ONLY -> {
                    fields.add(switch (v) {
                        case Double n -> new SortedNumericDocValuesField("foo", NumericUtils.doubleToSortableLong(n));
                        case Float n -> new SortedNumericDocValuesField("foo", NumericUtils.doubleToSortableLong(n));
                        case Number n -> new SortedNumericDocValuesField("foo", n.longValue());
                        case String s -> new SortedSetDocValuesField("foo", new BytesRef(s));
                        default -> throw new UnsupportedOperationException();
                    });
                }
                case DEFAULT -> {
                    fields.add(switch (v) {
                        case Double n -> new DoubleField("foo", n, Field.Store.NO);
                        case Float n -> new DoubleField("foo", n, Field.Store.NO);
                        case Number n -> new LongField("foo", n.longValue(), Field.Store.NO);
                        case String s -> new KeywordField("foo", s, Field.Store.NO);
                        default -> throw new UnsupportedOperationException();
                    });
                }
                default -> throw new IllegalStateException();
            }
        }
        if (count > 0) {
            fields.add(NumericDocValuesField.indexedField("foo" + COUNT_FIELD_SUFFIX, count));
            fields.add(mvField);
        }
        return fields;
    }
}
