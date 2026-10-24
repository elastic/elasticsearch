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
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;

public class LongRangeFieldMapperTests extends RangeFieldMapperTests {

    @Override
    protected XContentBuilder rangeSource(XContentBuilder in) throws IOException {
        return rangeSource(in, "1", "3");
    }

    @Override
    protected String storedValue() {
        return "1 : 3";
    }

    @Override
    protected Object rangeValue() {
        return 2;
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "long_range");
    }

    @Override
    protected TestRange<Long> randomRangeForSyntheticSourceTest() {
        var includeFrom = randomBoolean();
        Long from = randomLongBetween(Long.MIN_VALUE, Long.MAX_VALUE - 1);
        var includeTo = randomBoolean();
        Long to = randomLongBetween(from + 1, Long.MAX_VALUE);

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
        long from = randomLongBetween(Long.MIN_VALUE + 1, Long.MAX_VALUE - 1);
        long to = randomLongBetween(from, Long.MAX_VALUE - 1);
        return Tuple.tuple(from, to);
    }

    public void testFetchManyFromDocValues() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));
        List<Object> ranges = List.of(Map.of("gte", 10, "lte", 20), Map.of("gt", 1, "lt", 5), Map.of("gte", 10, "lte", 20));
        assertThat(
            docValueFields(mapperService, ranges, null),
            containsInAnyOrder(Map.of("gte", 10L, "lte", 20L), Map.of("gte", 2L, "lte", 4L))
        );
    }

    @Override
    protected RangeType rangeType() {
        return RangeType.LONG;
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }
}
