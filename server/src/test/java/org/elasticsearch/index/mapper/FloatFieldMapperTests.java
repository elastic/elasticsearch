/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.List;

public class FloatFieldMapperTests extends NumberFieldMapperTests {

    @Override
    protected Number missingValue() {
        return 123f;
    }

    @Override
    protected List<NumberTypeOutOfRangeSpec> outOfRangeSpecs() {
        return List.of(
            NumberTypeOutOfRangeSpec.of(NumberFieldMapper.NumberType.FLOAT, "3.4028235E39", "[float] supports only finite values"),
            NumberTypeOutOfRangeSpec.of(NumberFieldMapper.NumberType.FLOAT, "-3.4028235E39", "[float] supports only finite values"),
            NumberTypeOutOfRangeSpec.of(NumberFieldMapper.NumberType.FLOAT, Float.NaN, "[float] supports only finite values"),
            NumberTypeOutOfRangeSpec.of(NumberFieldMapper.NumberType.FLOAT, Float.POSITIVE_INFINITY, "[float] supports only finite values"),
            NumberTypeOutOfRangeSpec.of(NumberFieldMapper.NumberType.FLOAT, Float.NEGATIVE_INFINITY, "[float] supports only finite values")
        );
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "float");
    }

    @Override
    protected Number randomNumber() {
        /*
         * The source parser and doc values round trip will both reduce
         * the precision to 32 bits if the value is more precise.
         * randomDoubleBetween will smear the values out across a wide
         * range of valid values.
         */
        return randomBoolean() ? randomDoubleBetween(-Float.MAX_VALUE, Float.MAX_VALUE, true) : randomFloat();
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed) {
        return new NumberSyntheticSourceSupport(Number::floatValue, ignoreMalformed);
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupportForKeepTests(boolean ignoreMalformed, Mapper.SourceKeepMode sourceKeepMode) {
        return new NumberSyntheticSourceSupportForKeepTests(Number::floatValue, ignoreMalformed, sourceKeepMode);

    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }
}
