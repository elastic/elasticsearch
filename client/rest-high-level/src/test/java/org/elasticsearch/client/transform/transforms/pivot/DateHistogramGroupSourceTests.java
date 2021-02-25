/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms.pivot;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.function.Predicate;

public class DateHistogramGroupSourceTests extends AbstractXContentTestCase<DateHistogramGroupSource> {

    public static DateHistogramGroupSource.Interval randomDateHistogramInterval() {
        if (randomBoolean()) {
            return new DateHistogramGroupSource.FixedInterval(new DateHistogramInterval(randomPositiveTimeValue()));
        } else {
            return new DateHistogramGroupSource.CalendarInterval(new DateHistogramInterval(randomTimeValue(1, 1, "m", "h", "d", "w")));
        }
    }

    public static DateHistogramGroupSource randomDateHistogramGroupSource() {
        String field = randomBoolean() ? randomAlphaOfLengthBetween(1, 20) : null;
        Script script = randomBoolean() ? new Script(randomAlphaOfLengthBetween(1, 10)) : null;

        return new DateHistogramGroupSource(
            field,
            script,
            randomBoolean(),
            randomDateHistogramInterval(),
            randomBoolean() ? randomZone() : null
        );
    }

    @Override
    protected DateHistogramGroupSource createTestInstance() {
        return randomDateHistogramGroupSource();
    }

    @Override
    protected DateHistogramGroupSource doParseInstance(XContentParser parser) throws IOException {
        return DateHistogramGroupSource.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // allow unknown fields in the root of the object only
        return field -> field.isEmpty() == false;
    }
}
