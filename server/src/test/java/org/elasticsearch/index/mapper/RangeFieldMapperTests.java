/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.lookup.SourceProvider;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.RangeQueryBuilder.GTE_FIELD;
import static org.elasticsearch.index.query.RangeQueryBuilder.GT_FIELD;
import static org.elasticsearch.index.query.RangeQueryBuilder.LTE_FIELD;
import static org.elasticsearch.index.query.RangeQueryBuilder.LT_FIELD;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.startsWith;

public abstract class RangeFieldMapperTests extends MapperTestCase {

    protected static final String DATE_FORMAT = "uuuu-MM-dd HH:mm:ss.SSS||yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis";

    @Override
    protected boolean supportsSearchLookup() {
        return false;
    }

    @Override
    protected boolean supportsIgnoreMalformed() {
        return false;
    }

    public void testExistsQueryDocValuesDisabled() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("doc_values", false);
        }));
        assertExistsQuery(mapperService);
        assertParseMinimalWarnings();
    }

    public void testAggregationsDocValuesDisabled() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("doc_values", false);
        }));
        assertAggregatableConsistency(mapperService.fieldType("field"));
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("doc_values", b -> b.field("doc_values", false));
        checker.registerConflictCheck("index", b -> b.field("index", false));
        checker.registerConflictCheck("store", b -> b.field("store", true));
        checker.registerUpdateCheck(b -> b.field("coerce", false), m -> assertFalse(((RangeFieldMapper) m).coerce()));
    }

    private String getFromField() {
        return random().nextBoolean() ? GT_FIELD.getPreferredName() : GTE_FIELD.getPreferredName();
    }

    private String getToField() {
        return random().nextBoolean() ? LT_FIELD.getPreferredName() : LTE_FIELD.getPreferredName();
    }

    protected abstract XContentBuilder rangeSource(XContentBuilder in) throws IOException;

    protected final XContentBuilder rangeSource(XContentBuilder in, String lower, String upper) throws IOException {
        return in.startObject("field").field(getFromField(), lower).field(getToField(), upper).endObject();
    }

    @Override
    protected final Object getSampleValueForDocument() {
        return Collections.emptyMap();
    }

    @Override
    protected final Object getSampleObjectForDocument() {
        return getSampleValueForDocument();
    }

    @Override
    protected Object getSampleValueForQuery() {
        return rangeValue();
    }

    public final void testDefaults() throws Exception {

        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        ParsedDocument doc = mapper.parse(source(this::rangeSource));

        List<IndexableField> fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.size());
        IndexableField dvField = fields.get(0);
        assertEquals(DocValuesType.BINARY, dvField.fieldType().docValuesType());

        IndexableField pointField = fields.get(1);
        assertEquals(2, pointField.fieldType().pointIndexDimensionCount());
        assertFalse(pointField.fieldType().stored());
    }

    public final void testNotIndexed() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.field("index", false);
        }));
        ParsedDocument doc = mapper.parse(source(this::rangeSource));
        List<IndexableField> fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.size());
    }

    public final void testNoDocValues() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.field("doc_values", false);
        }));
        ParsedDocument doc = mapper.parse(source(this::rangeSource));

        List<IndexableField> fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.size());
        IndexableField pointField = fields.get(0);
        assertEquals(2, pointField.fieldType().pointIndexDimensionCount());
    }

    protected abstract String storedValue();

    public final void testStore() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.field("store", true);
        }));
        ParsedDocument doc = mapper.parse(source(this::rangeSource));
        List<IndexableField> fields = doc.rootDoc().getFields("field");
        assertEquals(3, fields.size());
        IndexableField dvField = fields.get(0);
        assertEquals(DocValuesType.BINARY, dvField.fieldType().docValuesType());
        IndexableField pointField = fields.get(1);
        assertEquals(2, pointField.fieldType().pointIndexDimensionCount());
        IndexableField storedField = fields.get(2);
        assertTrue(storedField.fieldType().stored());
        assertThat(storedField.stringValue(), containsString(storedValue()));
    }

    protected boolean supportsCoerce() {
        return true;
    }

    public final void testCoerce() throws IOException {
        assumeTrue("Type does not support coerce", supportsCoerce());

        DocumentMapper mapper2 = createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.field("coerce", false);
        }));

        Exception e = expectThrows(
            DocumentParsingException.class,
            () -> mapper2.parse(source(b -> b.startObject("field").field(getFromField(), "5.2").field(getToField(), "10").endObject()))
        );
        assertThat(e.getCause().getMessage(), containsString("passed as String"));
    }

    protected boolean supportsDecimalCoerce() {
        return true;
    }

    public final void testDecimalCoerce() throws IOException {
        assumeTrue("Type does not support decimal coerce", supportsDecimalCoerce());
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        ParsedDocument doc1 = mapper.parse(
            source(
                b -> b.startObject("field")
                    .field(GT_FIELD.getPreferredName(), "2.34")
                    .field(LT_FIELD.getPreferredName(), "5.67")
                    .endObject()
            )
        );

        ParsedDocument doc2 = mapper.parse(
            source(b -> b.startObject("field").field(GT_FIELD.getPreferredName(), "2").field(LT_FIELD.getPreferredName(), "5").endObject())
        );

        List<IndexableField> fields1 = doc1.rootDoc().getFields("field");
        List<IndexableField> fields2 = doc2.rootDoc().getFields("field");

        assertEquals(fields1.get(1).binaryValue(), fields2.get(1).binaryValue());
    }

    protected abstract Object rangeValue();

    public final void testNullField() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(b -> b.nullField("field")));
        assertNull(doc.rootDoc().get("field"));
    }

    private void assertNullBounds(CheckedConsumer<XContentBuilder, IOException> toCheck, boolean checkMin, boolean checkMax)
        throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.field("store", true);
        }));

        ParsedDocument doc1 = mapper.parse(source(toCheck));
        ParsedDocument doc2 = mapper.parse(source(b -> b.startObject("field").endObject()));
        String[] full = storedValue(doc2).split(" : ");
        String storedValue = storedValue(doc1);
        if (checkMin) {
            assertThat(storedValue, startsWith(full[0]));
        }
        if (checkMax) {
            assertThat(storedValue, endsWith(full[1]));
        }
    }

    private static String storedValue(ParsedDocument doc) {
        assertEquals(3, doc.rootDoc().getFields("field").size());
        List<IndexableField> fields = doc.rootDoc().getFields("field");
        IndexableField storedField = fields.get(2);
        return storedField.stringValue();
    }

    public final void testNullBounds() throws IOException {

        // null, null => min, max
        assertNullBounds(b -> b.startObject("field").nullField("gte").nullField("lte").endObject(), true, true);

        // null, val => min, val
        Object val = rangeValue();
        assertNullBounds(b -> b.startObject("field").nullField("gte").field("lte", val).endObject(), true, false);

        // val, null -> val, max
        assertNullBounds(b -> b.startObject("field").field("gte", val).nullField("lte").endObject(), false, true);
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed) {
        assumeTrue("test setup only supports numeric ranges", rangeType().isNumeric());

        return new SyntheticSourceSupport() {
            @Override
            public SyntheticSourceExample example(int maxValues) throws IOException {
                if (randomBoolean()) {
                    var range = randomRangeForSyntheticSourceTest();
                    return new SyntheticSourceExample(range.toInput(), range.toExpectedSyntheticSource(), this::mapping);
                }

                var values = randomList(1, maxValues, () -> randomRangeForSyntheticSourceTest());
                List<Object> in = values.stream().map(TestRange::toInput).toList();
                List<Object> outList = values.stream().sorted(Comparator.naturalOrder()).map(TestRange::toExpectedSyntheticSource).toList();
                Object out = outList.size() == 1 ? outList.get(0) : outList;

                return new SyntheticSourceExample(in, out, this::mapping);
            }

            private void mapping(XContentBuilder b) throws IOException {
                b.field("type", rangeType().name);
                if (rarely()) {
                    b.field("index", false);
                }
                if (rarely()) {
                    b.field("store", false);
                }
                if (rangeType() == RangeType.DATE) {
                    b.field("format", DATE_FORMAT);
                }
            }

            @Override
            public List<SyntheticSourceInvalidExample> invalidExample() throws IOException {
                return List.of();
            }
        };
    }

    /**
     * Stores range information as if it was provided by user.
     * Provides an expected value of provided range in synthetic source.
     * @param <T>
     */
    protected class TestRange<T extends Comparable<T>> implements Comparable<TestRange<T>> {
        private final RangeType type;
        private final T from;
        private final T to;
        private final boolean includeFrom;
        private final boolean includeTo;

        private final boolean skipDefaultFrom = randomBoolean();
        private final boolean skipDefaultTo = randomBoolean();

        public TestRange(RangeType type, T from, T to, boolean includeFrom, boolean includeTo) {
            this.type = type;
            this.from = from;
            this.to = to;
            this.includeFrom = includeFrom;
            this.includeTo = includeTo;
        }

        Object toInput() {
            var fromKey = includeFrom ? "gte" : "gt";
            var toKey = includeTo ? "lte" : "lt";

            return (ToXContent) (builder, params) -> {
                builder.startObject();
                if (includeFrom && from == null && skipDefaultFrom) {
                    // skip field entirely since it is equivalent to a default value
                } else {
                    builder.field(fromKey, from);
                }

                if (includeTo && to == null && skipDefaultTo) {
                    // skip field entirely since it is equivalent to a default value
                } else {
                    builder.field(toKey, to);
                }

                return builder.endObject();
            };
        }

        Object toExpectedSyntheticSource() {
            // When ranges are stored, they are always normalized to include both ends.
            // Also, "to" field always comes first.
            Map<String, Object> output = new LinkedHashMap<>();

            if (includeFrom) {
                if (from == null || from == rangeType().minValue()) {
                    output.put("gte", null);
                } else {
                    output.put("gte", from);
                }
            } else {
                var fromWithDefaults = from != null ? from : rangeType().minValue();
                output.put("gte", type.nextUp(fromWithDefaults));
            }

            if (includeTo) {
                if (to == null || to == rangeType().maxValue()) {
                    output.put("lte", null);
                } else {
                    output.put("lte", to);
                }
            } else {
                var toWithDefaults = to != null ? to : rangeType().maxValue();
                output.put("lte", type.nextDown(toWithDefaults));
            }

            return output;
        }

        @Override
        public int compareTo(TestRange<T> o) {
            return Comparator.comparing((TestRange<T> r) -> r.from, Comparator.nullsFirst(Comparator.naturalOrder()))
                // `> a` is converted into `>= a + 1` and so included range end will be smaller in resulting source
                .thenComparing(r -> r.includeFrom, Comparator.reverseOrder())
                .thenComparing(r -> r.to, Comparator.nullsLast(Comparator.naturalOrder()))
                // `< a` is converted into `<= a - 1` and so included range end will be larger in resulting source
                .thenComparing(r -> r.includeTo)
                .compare(this, o);
        }
    }

    protected TestRange<?> randomRangeForSyntheticSourceTest() {
        throw new AssumptionViolatedException("Should only be called for specific range types");
    }

    protected Source getSourceFor(CheckedConsumer<XContentBuilder, IOException> mapping, List<?> inputValues) throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(mapping(mapping)).documentMapper();

        CheckedConsumer<XContentBuilder, IOException> input = b -> {
            b.field("field");
            if (inputValues.size() == 1) {
                b.value(inputValues.get(0));
            } else {
                b.startArray();
                for (var range : inputValues) {
                    b.value(range);
                }
                b.endArray();
            }
        };

        try (Directory directory = newDirectory()) {
            RandomIndexWriter iw = new RandomIndexWriter(random(), directory);
            LuceneDocument doc = mapper.parse(source(input)).rootDoc();
            iw.addDocument(doc);
            iw.close();
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                SourceProvider provider = SourceProvider.fromLookup(mapper.mappers(), null, SourceFieldMetrics.NOOP);
                Source syntheticSource = provider.getSource(getOnlyLeafReader(reader).getContext(), 0);

                return syntheticSource;
            }
        }
    }

    protected abstract RangeType rangeType();

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        // Doc value fetching crashes.
        // https://github.com/elastic/elasticsearch/issues/70269
        // TODO when we fix doc values fetcher we should add tests for date and ip ranges.
        assumeFalse("DocValuesFetcher doesn't work", true);
        return null;
    }
}
