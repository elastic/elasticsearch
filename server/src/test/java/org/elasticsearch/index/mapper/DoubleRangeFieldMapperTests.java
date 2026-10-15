/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class DoubleRangeFieldMapperTests extends RangeFieldMapperTests {
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
        b.field("type", "double_range");
    }

    @Override
    protected boolean supportsDecimalCoerce() {
        return false;
    }

    @Override
    protected TestRange<Double> randomRangeForSyntheticSourceTest() {
        var includeFrom = randomBoolean();
        Double from = randomDoubleBetween(-Double.MAX_VALUE, Double.MAX_VALUE, true);
        var includeTo = randomBoolean();
        Double to = randomDoubleBetween(from, Double.MAX_VALUE, false);

        if (rarely()) {
            from = null;
        }
        if (rarely()) {
            to = null;
        }

        return new TestRange<>(rangeType(), from, to, includeFrom, includeTo);
    }

    @Override
    protected Tuple<Object, Object> randomInclusiveBounds() {
        double from = randomDoubleBetween(-Double.MAX_VALUE, Double.MAX_VALUE, true);
        double to = randomDoubleBetween(from, Double.MAX_VALUE, true);
        return Tuple.tuple(from, to);
    }

    /**
     * Doc values only keep inclusive bounds, so an exclusive bound comes back as the adjacent inclusive value, and an open side
     * comes back as {@code null}. This matches synthetic source.
     */
    public void testFetchExclusiveAndOpenBoundsFromDocValues() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));
        Map<String, Object> expected = new HashMap<>();
        expected.put("gte", Math.nextUp(1.0));
        expected.put("lte", null);
        assertThat(docValueFields(mapperService, Map.of("gt", 1.0), null), equalTo(List.of(expected)));
    }

    @Override
    protected RangeType rangeType() {
        return RangeType.DOUBLE;
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }
}
