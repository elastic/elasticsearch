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
        checker.registerConflictCheck("format", b -> b.field("format", "0000"));
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

    public void testFormatIndexesZeroPaddedTerms() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "integer");
            b.field("format", "00000000");
        }));

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", 42)));
        List<IndexableField> fields = doc.rootDoc().getFields("field");

        // Should have a terms field (inverted index) with the formatted value
        long termsCount = fields.stream().filter(f -> f.fieldType().indexOptions().compareTo(IndexOptions.NONE) > 0).count();
        assertEquals(1, termsCount);
        IndexableField termsField = fields.stream()
            .filter(f -> f.fieldType().indexOptions().compareTo(IndexOptions.NONE) > 0)
            .findFirst()
            .get();
        assertEquals(new BytesRef("00000042"), termsField.binaryValue());

        // Should have doc values
        long dvCount = fields.stream().filter(f -> f.fieldType().docValuesType() != DocValuesType.NONE).count();
        assertEquals(1, dvCount);
        IndexableField dvField = fields.stream().filter(f -> f.fieldType().docValuesType() != DocValuesType.NONE).findFirst().get();
        assertEquals(42, dvField.numericValue().intValue());
    }

    public void testFormatRejectsNegativeValues() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "integer");
            b.field("format", "0000");
        }));

        DocumentParsingException e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> b.field("field", -1))));
        assertThat(e.getCause().getMessage(), containsString("does not accept negative values"));
    }

    public void testFormatOnlyAllowsZeroChars() {
        Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", "integer");
            b.field("format", "XXXX");
        })));
        assertThat(e.getMessage(), containsString("[format] must consist only of '0' characters"));
    }

    public void testFormatOnlyAllowedOnInteger() {
        Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", "long");
            b.field("format", "0000");
        })));
        assertThat(e.getMessage(), containsString("[format] is only supported on [integer] fields"));
    }

    public void testFormatRequiresIndex() {
        Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", "integer");
            b.field("format", "0000");
            b.field("index", false);
        })));
        assertThat(e.getMessage(), containsString("[format] requires that [index] is true"));
    }

    public void testFormatValueLongerThanFormat() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "integer");
            b.field("format", "0000");
        }));

        // A value longer than the format should not be truncated
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", 99999)));
        List<IndexableField> fields = doc.rootDoc().getFields("field");
        IndexableField termsField = fields.stream()
            .filter(f -> f.fieldType().indexOptions().compareTo(IndexOptions.NONE) > 0)
            .findFirst()
            .get();
        assertEquals(new BytesRef("99999"), termsField.binaryValue());
    }
}
