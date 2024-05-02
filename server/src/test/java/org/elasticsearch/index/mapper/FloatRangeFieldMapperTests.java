/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class FloatRangeFieldMapperTests extends RangeFieldMapperTests {

    public void testSyntheticSourceDefaultValues() throws IOException {
        // Default range ends for float are negative and positive infinity
        // and they can not pass `roundTripSyntheticSource` test.

        CheckedConsumer<XContentBuilder, IOException> mapping = b -> {
            b.startObject("field");
            minimalMapping(b);
            b.endObject();
        };

        var inputValues = List.of(
            (builder, params) -> builder.startObject().field("gte", (Float) null).field("lte", 10).endObject(),
            (builder, params) -> builder.startObject().field("lte", 20).endObject(),
            (builder, params) -> builder.startObject().field("gte", 10).field("lte", (Float) null).endObject(),
            (builder, params) -> builder.startObject().field("gte", 20).endObject(),
            (ToXContent) (builder, params) -> builder.startObject().endObject()
        );

        var expected = List.of(new LinkedHashMap<>() {
            {
                put("gte", "-Infinity");
                put("lte", 10.0);
            }
        }, new LinkedHashMap<>() {
            {
                put("gte", "-Infinity");
                put("lte", 20.0);
            }
        }, new LinkedHashMap<>() {
            {
                put("gte", "-Infinity");
                put("lte", "Infinity");
            }
        }, new LinkedHashMap<>() {
            {
                put("gte", 10.0);
                put("lte", "Infinity");
            }
        }, new LinkedHashMap<>() {
            {
                put("gte", 20.0);
                put("lte", "Infinity");
            }
        });

        var source = getSourceFor(mapping, inputValues);
        var actual = source.source().get("field");
        assertThat(actual, equalTo(expected));
    }

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
        var includeFrom = randomBoolean();
        Float from = (float) randomDoubleBetween(-Float.MAX_VALUE, Float.MAX_VALUE - Math.ulp(Float.MAX_VALUE), true);
        var includeTo = randomBoolean();
        Float to = (float) randomDoubleBetween(from + Math.ulp(from), Float.MAX_VALUE, true);

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
