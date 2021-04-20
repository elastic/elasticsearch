/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.index.mapper.NumberFieldTypeTests.OutOfRangeSpec;

import java.io.IOException;
import java.util.List;

public class ByteFieldMapperTests extends WholeNumberFieldMapperTests {
    @Override
    protected Number missingValue() {
        return 123;
    }

    @Override
    protected List<OutOfRangeSpec> outOfRangeSpecs() {
        return List.of(
            OutOfRangeSpec.of(NumberType.BYTE, "128", "is out of range for a byte"),
            OutOfRangeSpec.of(NumberType.BYTE, "-129", "is out of range for a byte"),
            OutOfRangeSpec.of(NumberType.BYTE, 128, "is out of range for a byte"),
            OutOfRangeSpec.of(NumberType.BYTE, -129, "is out of range for a byte")
        );
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "byte");
    }

    @Override
    protected Number randomNumber() {
        if (randomBoolean()) {
            return randomByte();
        }
        if (randomBoolean()) {
            return randomDouble();
        }
        return randomDoubleBetween(Byte.MIN_VALUE, Byte.MAX_VALUE, true);
    }
}
