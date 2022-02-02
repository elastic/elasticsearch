/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.index.mapper.NumberFieldTypeTests.OutOfRangeSpec;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class DoubleFieldMapperTests extends NumberFieldMapperTests {

    @Override
    protected Number missingValue() {
        return 123d;
    }

    @Override
    protected List<OutOfRangeSpec> outOfRangeSpecs() {
        return List.of(
            OutOfRangeSpec.of(NumberFieldMapper.NumberType.DOUBLE, "1.7976931348623157E309", "[double] supports only finite values"),
            OutOfRangeSpec.of(NumberFieldMapper.NumberType.DOUBLE, "-1.7976931348623157E309", "[double] supports only finite values"),
            OutOfRangeSpec.of(NumberFieldMapper.NumberType.DOUBLE, Double.NaN, "[double] supports only finite values"),
            OutOfRangeSpec.of(NumberFieldMapper.NumberType.DOUBLE, Double.POSITIVE_INFINITY, "[double] supports only finite values"),
            OutOfRangeSpec.of(NumberFieldMapper.NumberType.DOUBLE, Double.NEGATIVE_INFINITY, "[double] supports only finite values")
        );
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "double");
    }

    @Override
    protected boolean allowsIndexTimeScript() {
        return true;
    }

    @Override
    protected Number randomNumber() {
        /*
         * The source parser and doc values round trip will both increase
         * the precision to 64 bits if the value is less precise.
         * randomDoubleBetween will smear the values out across a wide
         * range of valid values.
         */
        return randomBoolean() ? randomDoubleBetween(-Double.MAX_VALUE, Double.MAX_VALUE, true) : randomFloat();
    }

    public void testScriptAndPrecludedParameters() {
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
                b.field("type", "double");
                b.field("script", "test");
                b.field("coerce", "true");
            })));
            assertThat(e.getMessage(), equalTo("Failed to parse mapping: Field [coerce] cannot be set in conjunction with field [script]"));
        }
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
                b.field("type", "double");
                b.field("script", "test");
                b.field("null_value", 7);
            })));
            assertThat(
                e.getMessage(),
                equalTo("Failed to parse mapping: Field [null_value] cannot be set in conjunction with field [script]")
            );
        }
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
                b.field("type", "double");
                b.field("script", "test");
                b.field("ignore_malformed", "true");
            })));
            assertThat(
                e.getMessage(),
                equalTo("Failed to parse mapping: Field [ignore_malformed] cannot be set in conjunction with field [script]")
            );
        }
    }
}
