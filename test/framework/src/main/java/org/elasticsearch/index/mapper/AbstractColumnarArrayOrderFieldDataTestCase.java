/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * Shared fielddata coverage for high-cardinality fields in strictly columnar mode that store their values in document order with inline
 * nulls ({@link MultiValuedBinaryDocValuesField.ArrayOrderDeduplicated}) instead of a sidecar {@code .offsets} field. Loads values through
 * the mapper's fielddata builder (so the mapper-to-fielddata wiring is exercised), which sorts within a document, drops nulls and keeps
 * duplicates. Concrete subclasses supply the field type (keyword, text, match_only_text).
 */
public abstract class AbstractColumnarArrayOrderFieldDataTestCase extends MapperServiceTestCase {

    protected abstract String fieldTypeName();

    @Override
    public void setUp() throws Exception {
        super.setUp();
        assumeTrue("columnar index mode requires a snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
    }

    private MapperService columnarMapperService() throws IOException {
        Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build();
        return createMapperService(settings, mapping(b -> b.startObject("field").field("type", fieldTypeName()).endObject()));
    }

    /**
     * Indexes a single document and returns the fielddata values exposed for it, or {@code null} when the field has no non-null values
     * for that document (an all-null or empty array, which fielddata reports as no values via {@code advanceExact == false}).
     */
    private List<String> fielddataValues(CheckedConsumer<XContentBuilder, IOException> doc) throws IOException {
        MapperService mapperService = columnarMapperService();
        // Sanity: this field must actually use the in-order binary doc-values format, otherwise the test is not exercising the bug.
        assertTrue(mapperService.documentMapper().mappers().getMapper("field").storesArrayValuesInOrder());

        List<String> values = new ArrayList<>();
        boolean[] hasValues = { false };
        withLuceneIndex(mapperService, iw -> iw.addDocument(mapperService.documentMapper().parse(source(doc)).rootDoc()), reader -> {
            LeafReaderContext leaf = reader.leaves().get(0);
            IndexFieldData<?> indexFieldData = mapperService.fieldType("field")
                .fielddataBuilder(FieldDataContext.noRuntimeFields("test", "test"))
                .build(new IndexFieldDataCache.None(), new NoneCircuitBreakerService());
            SortedBinaryDocValues docValues = indexFieldData.load(leaf).getBytesValues();
            if (docValues.advanceExact(0)) {
                hasValues[0] = true;
                int count = docValues.docValueCount();
                for (int i = 0; i < count; i++) {
                    values.add(docValues.nextValue().utf8ToString());
                }
            }
        });
        return hasValues[0] ? values : null;
    }

    /**
     * Generates {@code n} distinct fixed-length alphanumeric values. Equal-length ASCII alphanumerics sort identically as Java strings
     * and as {@link org.apache.lucene.util.BytesRef}, so callers can compute the expected fielddata order with natural string ordering.
     */
    private List<String> randomDistinctValues(int n) {
        LinkedHashSet<String> values = new LinkedHashSet<>();
        while (values.size() < n) {
            values.add(randomAlphanumericOfLength(8));
        }
        return new ArrayList<>(values);
    }

    public void testMultiValuedReadsBackSorted() throws IOException {
        List<String> input = randomDistinctValues(3);
        List<String> expected = new ArrayList<>(input);
        expected.sort(null);
        assertEquals(expected, fielddataValues(b -> b.array("field", input.toArray(new String[0]))));
    }

    public void testDuplicatesKept() throws IOException {
        List<String> distinct = randomDistinctValues(2);
        List<String> input = List.of(distinct.get(0), distinct.get(0), distinct.get(1));
        List<String> expected = new ArrayList<>(input);
        expected.sort(null);
        assertEquals(expected, fielddataValues(b -> b.array("field", input.toArray(new String[0]))));
    }

    public void testNullsDropped() throws IOException {
        List<String> distinct = randomDistinctValues(2);
        List<String> expected = new ArrayList<>(distinct);
        expected.sort(null);
        assertEquals(
            expected,
            fielddataValues(b -> b.startArray("field").value(distinct.get(0)).nullValue().value(distinct.get(1)).endArray())
        );
    }

    public void testSingleValue() throws IOException {
        String value = randomAlphanumericOfLength(8);
        assertEquals(List.of(value), fielddataValues(b -> b.field("field", value)));
    }

    public void testAllNullArrayHasNoValues() throws IOException {
        assertNull(fielddataValues(b -> b.startArray("field").nullValue().nullValue().endArray()));
    }

    public void testEmptyArrayHasNoValues() throws IOException {
        assertNull(fielddataValues(b -> b.startArray("field").endArray()));
    }
}
