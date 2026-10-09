/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.querydsl.query;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.compute.querydsl.query.SingleValueMatchQuery;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.compute.test.TestWarningsSource;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.codec.columnar.ColumnarDocValuesFormatSelector;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.ColumnarBinaryDocValuesField;
import org.elasticsearch.index.mapper.FieldMapper;
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
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

public class SingleValueMatchQueryTests extends MapperServiceTestCase {
    interface Setup {
        XContentBuilder mapping(XContentBuilder builder) throws IOException;

        List<List<Object>> build(RandomIndexWriter iw) throws IOException;

        void assertRewrite(IndexSearcher indexSearcher, Query query) throws IOException;

        /** Checks the setup reads its doc values the way it means to, so the query is run over the reader under test. */
        default void assertFieldData(IndexFieldData<?> fieldData, IndexReader reader) throws IOException {}

        /**
         * Index settings the mapper service is created with; HIGH-cardinality keyword setups use a strict-columnar mode so the field gets
         * binary doc values by default instead of relying on the removed {@code doc_values.cardinality} mapping option.
         */
        default Settings indexSettings() {
            return Settings.EMPTY;
        }

        /** Whether this setup writes the ColumNAR codec's payload, which it can only do where the codec is available. */
        default boolean needsColumnarCodec() {
            return false;
        }
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
                    if (fieldType.equals("keyword")) {
                        // Both layouts a strictly columnar index writes high-cardinality keywords in, since each has its own reader.
                        for (DocValuesMode highCardinality : new DocValuesMode[] {
                            DocValuesMode.DOC_VALUES_ONLY_HIGH_CARDINALITY,
                            DocValuesMode.DOC_VALUES_ONLY_HIGH_CARDINALITY_PAYLOAD }) {
                            params.add(new Object[] { new StandardSetup(fieldType, multivaluedField, highCardinality, allowEmpty, 100) });
                        }
                        if (multivaluedField == false) {
                            // A field that promises one value a document, which neither layout above is written for.
                            for (DocValuesMode singleValued : new DocValuesMode[] {
                                DocValuesMode.DOC_VALUES_ONLY_SINGLE_VALUED,
                                DocValuesMode.DOC_VALUES_ONLY_SINGLE_VALUED_COLUMNAR }) {
                                params.add(new Object[] { new StandardSetup(fieldType, false, singleValued, allowEmpty, 100) });
                            }
                        }
                    }
                }
            }
        }
        return params;
    }

    private final Setup setup;

    /**
     * Target for warnings.
     */
    private final DriverContext warningsContext = new DriverContext(
        BigArrays.NON_RECYCLING_INSTANCE,
        TestBlockFactory.getNonBreakingInstance(),
        null
    );

    public SingleValueMatchQueryTests(Setup setup) {
        this.setup = setup;
    }

    public void testQuery() throws IOException {
        assumeCodecAvailable();
        MapperService mapper = createMapperService(setup.indexSettings(), mapping(setup::mapping));
        try (Directory d = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), d)) {
            List<List<Object>> fieldValues = setup.build(iw);
            try (IndexReader reader = iw.getReader()) {
                SearchExecutionContext ctx = createSearchExecutionContext(mapper, new IndexSearcher(reader));
                IndexFieldData<?> fieldData = ctx.getForField(mapper.fieldType("foo"), MappedFieldType.FielddataOperation.SEARCH);
                setup.assertFieldData(fieldData, reader);
                withBoundQuery(fieldData, query -> {
                    runCase(fieldValues, ctx.searcher().count(query));
                    setup.assertRewrite(ctx.searcher(), query);
                });
            }
        }
    }

    public void testEmpty() throws IOException {
        assumeCodecAvailable();
        MapperService mapper = createMapperService(setup.indexSettings(), mapping(setup::mapping));
        try (Directory d = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), d)) {
            try (IndexReader reader = iw.getReader()) {
                SearchExecutionContext ctx = createSearchExecutionContext(mapper, new IndexSearcher(reader));
                withBoundQuery(
                    ctx.getForField(mapper.fieldType("foo"), MappedFieldType.FielddataOperation.SEARCH),
                    query -> runCase(List.of(), ctx.searcher().count(query))
                );
            }
        }
    }

    /** A setup that writes the codec's payload only runs where the codec can be turned on. */
    private void assumeCodecAvailable() {
        assumeTrue(
            "columnar_codec feature flag must be enabled",
            setup.needsColumnarCodec() == false || ColumnarDocValuesFormatSelector.COLUMNAR_CODEC_FEATURE_FLAG.isEnabled()
        );
    }

    @FunctionalInterface
    interface IOConsumer<T> {
        void accept(T t) throws IOException;
    }

    /**
     * Build a {@link SingleValueMatchQuery} bound, via a private one-off {@link QueryWarnings}
     * bridge, to a fresh {@link Warnings}, run {@code action} with it while the binding is active,
     * then close the binding. Mirrors how {@link org.elasticsearch.compute.operator.lookup.QueryList}
     * binds a non-shared query to its bridge, but scopes the binding to the action's lifetime.
     */
    private void withBoundQuery(IndexFieldData<?> fieldData, IOConsumer<SingleValueMatchQuery> action) throws IOException {
        QueryWarnings bridge = QueryWarnings.EMIT;
        SingleValueMatchQuery query = new SingleValueMatchQuery(
            fieldData,
            bridge,
            new TestWarningsSource("test"),
            "single-value function encountered multi-value"
        );
        try (Releasable ignored = bridge.bind(Map.of(query, warningsContext.createWarnings(new TestWarningsSource("test"))))) {
            action.accept(query);
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
            warningsContext.finish();
            assertThat(
                warningsContext.warnings(),
                containsInAnyOrder(
                    "Line 1:1: evaluation of [test] failed, treating result as null. Only first 20 failures recorded.",
                    "Line 1:1: java.lang.IllegalArgumentException: single-value function encountered multi-value"
                )
            );
        }
    }

    private record StandardSetup(String fieldType, boolean multivaluedField, DocValuesMode docValuesMode, boolean empty, int count)
        implements
            Setup {
        @Override
        public XContentBuilder mapping(XContentBuilder builder) throws IOException {
            return switch (docValuesMode) {
                // binary doc values are used for high cardinality fields in strictly columnar index modes
                case DOC_VALUES_ONLY_HIGH_CARDINALITY, DOC_VALUES_ONLY_HIGH_CARDINALITY_PAYLOAD, DOC_VALUES_ONLY_SINGLE_VALUED,
                    DOC_VALUES_ONLY_SINGLE_VALUED_COLUMNAR -> builder.startObject("foo").field("type", fieldType).endObject();
                case DOC_VALUES_ONLY -> builder.startObject("foo").field("type", fieldType).field("doc_values", true).endObject();
                case DEFAULT -> builder.startObject("foo").field("type", fieldType).endObject();
            };
        }

        @Override
        public Settings indexSettings() {
            // The HIGH-cardinality keyword setups rely on a strict-columnar index mode to default the field to binary doc values.
            // Which of the two layouts it gets follows the codec setting, and these build the doc values by hand, so the setting is
            // named rather than left to its default.
            return switch (docValuesMode) {
                case DOC_VALUES_ONLY_HIGH_CARDINALITY, DOC_VALUES_ONLY_HIGH_CARDINALITY_PAYLOAD, DOC_VALUES_ONLY_SINGLE_VALUED,
                    DOC_VALUES_ONLY_SINGLE_VALUED_COLUMNAR -> Settings.builder()
                        .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
                        .put(IndexSettings.COLUMNAR_CODEC_ENABLED_SETTING.getKey(), docValuesMode.columnarCodec())
                        .put(FieldMapper.DOC_VALUES_MULTI_VALUE_SETTING.getKey(), docValuesMode.singleValuedBlobs() == false)
                        .build();
                case DEFAULT, DOC_VALUES_ONLY -> Settings.EMPTY;
            };
        }

        @Override
        public boolean needsColumnarCodec() {
            return docValuesMode.columnarCodec();
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
        public void assertFieldData(IndexFieldData<?> fieldData, IndexReader reader) throws IOException {
            if (docValuesMode.singleValuedBlobs()) {
                // A blob is one value, so the documents holding a blob are handed to the query as they are.
                for (LeafReaderContext leaf : reader.leaves()) {
                    assertThat(fieldData.load(leaf).getBytesValues().singleValuedDocs(), notNullValue());
                }
            }
        }

        @Override
        public void assertRewrite(IndexSearcher indexSearcher, Query query) throws IOException {
            // The high-cardinality setups write their doc values through the test's own codec rather than as a column, and only a
            // column says whether its documents each hold one value, so the query never rewrites away.
            if (docValuesMode.highCardinality() == false && empty == false && multivaluedField == false) {
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
        /** Binary doc values in the in-order column, with the slot count in a companion {@code .counts} field. */
        DOC_VALUES_ONLY_HIGH_CARDINALITY,
        /** Binary doc values as the ColumNAR codec's payload, which carries its own slot count. */
        DOC_VALUES_ONLY_HIGH_CARDINALITY_PAYLOAD,
        /** Binary doc values of a {@code multi_value: false} field: each document's blob is its one value's own bytes. */
        DOC_VALUES_ONLY_SINGLE_VALUED,
        /** The same blobs, on an index with the ColumNAR codec on, where the field is framed as bare values. */
        DOC_VALUES_ONLY_SINGLE_VALUED_COLUMNAR;

        boolean highCardinality() {
            return this != DEFAULT && this != DOC_VALUES_ONLY;
        }

        boolean singleValuedBlobs() {
            return this == DOC_VALUES_ONLY_SINGLE_VALUED || this == DOC_VALUES_ONLY_SINGLE_VALUED_COLUMNAR;
        }

        boolean columnarCodec() {
            return this == DOC_VALUES_ONLY_HIGH_CARDINALITY_PAYLOAD || this == DOC_VALUES_ONLY_SINGLE_VALUED_COLUMNAR;
        }
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
        // High-cardinality keyword fields in a strictly columnar index write one of two binary layouts, each read by its own reader:
        // the ArrayOrderInlineNull format ([len+1][val] slots in document order) with a companion count, or, under the ColumNAR
        // codec, a payload carrying its own count. The setup names which one, so the bytes match the reader the field type selects.
        var mvField = new MultiValuedBinaryDocValuesField.ArrayOrderInlineNull("foo");
        var payloadField = new ColumnarBinaryDocValuesField("foo", MultiValuedBinaryDocValuesField.ValueOrdering.UNSORTED);
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
                case DOC_VALUES_ONLY_HIGH_CARDINALITY_PAYLOAD -> {
                    switch (v) {
                        case String s -> {
                            payloadField.add(new BytesRef(s));
                            count++;
                        }
                        default -> throw new UnsupportedOperationException();
                    }
                }
                case DOC_VALUES_ONLY_SINGLE_VALUED, DOC_VALUES_ONLY_SINGLE_VALUED_COLUMNAR -> {
                    switch (v) {
                        // The blob is the value's own bytes, with no count beside it.
                        case String s -> fields.add(new BinaryDocValuesField("foo", new BytesRef(s)));
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
            if (docValuesMode == DocValuesMode.DOC_VALUES_ONLY_HIGH_CARDINALITY_PAYLOAD) {
                // The payload states the count itself, so it travels alone.
                fields.add(payloadField);
            } else {
                fields.add(NumericDocValuesField.indexedField("foo" + COUNT_FIELD_SUFFIX, count));
                fields.add(mvField);
            }
        }
        return fields;
    }
}
