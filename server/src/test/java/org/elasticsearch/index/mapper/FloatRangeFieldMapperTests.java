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

public class FloatRangeFieldMapperTests extends RangeFieldMapperTests {
    @Override
    protected XContentBuilder rangeSource(XContentBuilder in) throws IOException {
        return rangeSource(in, "0.5", "2.7");
    }

    @Override
    protected String storedValue() {
        return "0.5 : 2.7";
    }

    @Override
    protected Object rangeValue() {
        return 2.7;
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "float_range");
    }

    @Override
    protected boolean supportsDecimalCoerce() {
        return false;
    }

    @Override
    protected TestRange<Float> randomRangeForSyntheticSourceTest() {
        Float from = (float) randomDoubleBetween(-Float.MAX_VALUE, Float.MAX_VALUE - Math.ulp(Float.MAX_VALUE), true);
        Float to = (float) randomDoubleBetween(from + Math.ulp(from), Float.MAX_VALUE, true);
        boolean valuesAdjacent = Math.nextUp(from) > Math.nextDown(to);
        var includeFrom = valuesAdjacent || randomBoolean();
        var includeTo = valuesAdjacent || randomBoolean();

        if (rarely()) {
            from = null;
        }
        if (rarely()) {
            to = null;
        }

        return new TestRange<>(rangeType(), from, to, includeFrom, includeTo);
    }

    @Override
    protected RangeType rangeType() {
        return RangeType.FLOAT;
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }
}
