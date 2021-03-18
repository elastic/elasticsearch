/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.NumberFieldTypeTests.OutOfRangeSpec;

import java.io.IOException;
import java.util.List;

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
}
