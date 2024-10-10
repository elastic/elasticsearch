/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class DateRangeFieldMapperTests extends RangeFieldMapperTests {
    private static final String FROM_DATE = "2016-10-31";
    private static final String TO_DATE = "2016-11-01 20:00:00";

    @Override
    protected XContentBuilder rangeSource(XContentBuilder in) throws IOException {
        return in.startObject("field").field("gt", FROM_DATE).field("lt", TO_DATE).endObject();
    }

    @Override
    protected String storedValue() {
        return "1477958399999";
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "date_range");
        b.field("format", DATE_FORMAT);
    }

    @Override
    protected boolean supportsCoerce() {
        return false;
    }

    @Override
    protected Object rangeValue() {
        return "1477872000000";
    }

    public void testIllegalFormatField() {
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(fieldMapping(b -> b.field("type", "date_range").array("format", "test_format")))
        );
        assertThat(e.getMessage(), containsString("Invalid format: [[test_format]]: Unknown pattern letter: t"));
    }

    @Override
    @SuppressWarnings("unchecked")
    protected TestRange<Long> randomRangeForSyntheticSourceTest() {
        var includeFrom = randomBoolean();
        Long from = rarely() ? null : randomLongBetween(0, DateUtils.MAX_MILLIS_BEFORE_9999 - 1);
        var includeTo = randomBoolean();
        Long to = rarely() ? null : randomLongBetween((from == null ? 0 : from) + 1, DateUtils.MAX_MILLIS_BEFORE_9999);

        return new TestRange<>(rangeType(), from, to, includeFrom, includeTo) {
            private final DateFormatter inputDateFormatter = DateFormatter.forPattern("yyyy-MM-dd HH:mm:ss.SSS");
            private final DateFormatter expectedDateFormatter = DateFormatter.forPattern(DATE_FORMAT);

            @Override
            Object toInput() {
                var fromKey = includeFrom ? "gte" : "gt";
                var toKey = includeTo ? "lte" : "lt";

                var fromFormatted = from != null && randomBoolean() ? inputDateFormatter.format(Instant.ofEpochMilli(from)) : from;
                var toFormatted = to != null && randomBoolean() ? inputDateFormatter.format(Instant.ofEpochMilli(to)) : to;

                return (ToXContent) (builder, params) -> builder.startObject()
                    .field(fromKey, fromFormatted)
                    .field(toKey, toFormatted)
                    .endObject();
            }

            @Override
            Object toExpectedSyntheticSource() {
                Map<String, Object> expectedInMillis = (Map<String, Object>) super.toExpectedSyntheticSource();

                Map<String, Object> expectedFormatted = new HashMap<>();
                for (var entry : expectedInMillis.entrySet()) {
                    expectedFormatted.put(
                        entry.getKey(),
                        entry.getValue() != null ? expectedDateFormatter.format(Instant.ofEpochMilli((long) entry.getValue())) : null
                    );
                }

                return expectedFormatted;
            }
        };
    }

    @Override
    protected RangeType rangeType() {
        return RangeType.DATE;
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }
}
