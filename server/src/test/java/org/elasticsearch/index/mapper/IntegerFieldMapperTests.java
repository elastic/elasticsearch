/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.List;

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
}
