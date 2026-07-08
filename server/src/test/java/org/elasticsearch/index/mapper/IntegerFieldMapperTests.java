/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class IntegerFieldMapperTests extends WholeNumberFieldMapperTests {

    @Override
    protected Number missingValue() {
        return 123;
    }

    @Override
    protected List<NumberTypeOutOfRangeSpec> outOfRangeSpecs() {
        return List.of(
            NumberTypeOutOfRangeSpec.of(NumberType.INTEGER, "2147483648", "is out of range for an integer"),
            NumberTypeOutOfRangeSpec.of(NumberType.INTEGER, "-2147483649", "is out of range for an integer"),
            NumberTypeOutOfRangeSpec.of(NumberType.INTEGER, 2147483648L, " out of range of int"),
            NumberTypeOutOfRangeSpec.of(NumberType.INTEGER, -2147483649L, " out of range of int")
        );
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        super.registerParameters(checker);
        checker.registerConflictCheck("index_terms", b -> b.field("index_terms", true));
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "integer");
    }

    @Override
    protected Number randomNumber() {
        if (randomBoolean()) {
            return randomInt();
        }
        if (randomBoolean()) {
            return randomDouble();
        }
        return randomDoubleBetween(Integer.MIN_VALUE, Integer.MAX_VALUE, true);
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }

    protected boolean supportsBulkIntBlockReading() {
        return true;
    }

    @Override
    protected Object[] getThreeSampleValues() {
        return new Object[] { 1, 2, 3 };
    }

    public void testIndexTermsIndexesSortableBytesTerms() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "integer");
            b.field("index_terms", true);
        }));

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", 42)));
        List<IndexableField> fields = doc.rootDoc().getFields("field");

        // Should have a terms field (inverted index) with the sortable-bytes encoded value
        long termsCount = fields.stream().filter(f -> f.fieldType().indexOptions().compareTo(IndexOptions.NONE) > 0).count();
        assertEquals(1, termsCount);
        IndexableField termsField = fields.stream()
            .filter(f -> f.fieldType().indexOptions().compareTo(IndexOptions.NONE) > 0)
            .findFirst()
            .get();
        byte[] expected = new byte[Integer.BYTES];
        NumericUtils.intToSortableBytes(42, expected, 0);
        assertEquals(new BytesRef(expected), termsField.binaryValue());

        // Should have doc values
        long dvCount = fields.stream().filter(f -> f.fieldType().docValuesType() != DocValuesType.NONE).count();
        assertEquals(1, dvCount);
        IndexableField dvField = fields.stream().filter(f -> f.fieldType().docValuesType() != DocValuesType.NONE).findFirst().get();
        assertEquals(42, dvField.numericValue().intValue());
    }

    public void testIndexTermsIndexesNegativeValues() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "integer");
            b.field("index_terms", true);
        }));

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", -1)));
        List<IndexableField> fields = doc.rootDoc().getFields("field");

        IndexableField termsField = fields.stream()
            .filter(f -> f.fieldType().indexOptions().compareTo(IndexOptions.NONE) > 0)
            .findFirst()
            .get();
        byte[] expected = new byte[Integer.BYTES];
        NumericUtils.intToSortableBytes(-1, expected, 0);
        assertEquals(new BytesRef(expected), termsField.binaryValue());

        IndexableField dvField = fields.stream().filter(f -> f.fieldType().docValuesType() != DocValuesType.NONE).findFirst().get();
        assertEquals(-1, dvField.numericValue().intValue());
    }

    public void testIndexTermsOnlyAllowedOnInteger() {
        Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", "long");
            b.field("index_terms", true);
        })));
        assertThat(e.getMessage(), containsString("[index_terms] is only supported on [integer] fields"));
    }

    public void testIndexTermsRequiresIndex() {
        Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", "integer");
            b.field("index_terms", true);
            b.field("index", false);
        })));
        assertThat(e.getMessage(), containsString("[index_terms] requires that [index] is true"));
    }
}
