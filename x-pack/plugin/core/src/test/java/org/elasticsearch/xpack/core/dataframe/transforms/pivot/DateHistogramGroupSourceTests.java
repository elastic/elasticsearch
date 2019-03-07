/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms.pivot;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

public class DateHistogramGroupSourceTests extends AbstractSerializingTestCase<DateHistogramGroupSource> {

    public static DateHistogramGroupSource randomDateHistogramGroupSource() {
        String field = randomAlphaOfLengthBetween(1, 20);
        DateHistogramGroupSource dateHistogramGroupSource = new DateHistogramGroupSource(field);
        if (randomBoolean()) {
            dateHistogramGroupSource.setInterval(randomLongBetween(1, 10_000));
        } else {
            dateHistogramGroupSource.setDateHistogramInterval(randomFrom(DateHistogramInterval.days(10),
                DateHistogramInterval.minutes(1), DateHistogramInterval.weeks(1)));
        }
        if (randomBoolean()) {
            dateHistogramGroupSource.setTimeZone(randomZone());
        }
        if (randomBoolean()) {
            dateHistogramGroupSource.setFormat(randomAlphaOfLength(10));
        }
        return dateHistogramGroupSource;
    }

    @Override
    protected DateHistogramGroupSource doParseInstance(XContentParser parser) throws IOException {
        return DateHistogramGroupSource.fromXContent(parser, false);
    }

    @Override
    protected DateHistogramGroupSource createTestInstance() {
        return randomDateHistogramGroupSource();
    }

    @Override
    protected Reader<DateHistogramGroupSource> instanceReader() {
        return DateHistogramGroupSource::new;
    }

}
